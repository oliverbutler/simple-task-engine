package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// TaskResult represents the result of a task execution
type TaskResult struct {
	Task  *Task
	Error error
}

// InFlightTask represents a task that is currently being processed
type InFlightTask struct {
	Task      *Task
	StartTime time.Time
}

// Task represents a task from the task_pool table
type Task struct {
	ID             string          `json:"id"`
	IdempotencyKey sql.NullString  `json:"idempotency_key"`
	Type           string          `json:"type"`
	Priority       string          `json:"priority"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	LockedUntil    sql.NullTime    `json:"locked_until"`
	RetryCount     int             `json:"retry_count"`
	MaxRetryCount  int             `json:"max_retry_count"`
	LastError      sql.NullString  `json:"last_error"`
	ProcessAfter   time.Time       `json:"process_after"`
	CorrelationID  sql.NullString  `json:"correlation_id"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// Config holds application configuration
type Config struct {
	DBConnectionString string
	LockDuration       time.Duration
	PollInterval       time.Duration
	APIEndpoint        string
	MaxConcurrent      int
}

// TaskProcessor handles task processing
type TaskProcessor struct {
	db            *sql.DB
	config        Config
	wg            sync.WaitGroup
	stop          chan struct{}
	inFlightTasks map[string]*InFlightTask
	tasksMutex    sync.RWMutex
	resultsChan   chan TaskResult
	batchTicker   *time.Ticker
	shutdownMode  bool         // Flag to indicate we're in shutdown mode
	shutdownMutex sync.RWMutex // Mutex for the shutdown flag
}

// isShutdownMode safely checks if the processor is in shutdown mode
func (tp *TaskProcessor) isShutdownMode() bool {
	tp.shutdownMutex.RLock()
	defer tp.shutdownMutex.RUnlock()
	return tp.shutdownMode
}

// setShutdownMode safely sets the shutdown mode
func (tp *TaskProcessor) setShutdownMode(mode bool) {
	tp.shutdownMutex.Lock()
	defer tp.shutdownMutex.Unlock()
	tp.shutdownMode = mode
}

// getInFlightTaskCount safely gets the number of in-flight tasks
func (tp *TaskProcessor) getInFlightTaskCount() int {
	tp.tasksMutex.RLock()
	defer tp.tasksMutex.RUnlock()
	return len(tp.inFlightTasks)
}

// NewTaskProcessor creates a new task processor
func NewTaskProcessor(config Config) (*TaskProcessor, error) {
	db, err := sql.Open("mysql", config.DBConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Configure connection pool - use more conservative settings
	db.SetMaxOpenConns(15)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(1 * time.Minute)
	db.SetConnMaxIdleTime(30 * time.Second)

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Create the task_pool table if it doesn't exist
	if err := createTaskPoolTableIfNotExists(db); err != nil {
		return nil, fmt.Errorf("failed to create task_pool table: %w", err)
	}

	// Make the results channel larger to avoid blocking during shutdown
	resultsBufferSize := config.MaxConcurrent * 2

	processor := &TaskProcessor{
		db:            db,
		config:        config,
		stop:          make(chan struct{}),
		inFlightTasks: make(map[string]*InFlightTask),
		resultsChan:   make(chan TaskResult, resultsBufferSize),
		batchTicker:   time.NewTicker(1 * time.Second), // Process results every second
		shutdownMode:  false,
	}

	// Start a connection monitor goroutine
	go processor.monitorDatabaseConnection()

	return processor, nil
}

// monitorDatabaseConnection periodically checks the database connection
// and attempts to reconnect if necessary
func (tp *TaskProcessor) monitorDatabaseConnection() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-tp.stop:
			return
		case <-ticker.C:
			if err := tp.db.Ping(); err != nil {
				log.Printf("Database connection check failed: %v, attempting to reconnect", err)

				// Force close and reopen the connection
				tp.db.Close()

				// Reopen with a short delay
				time.Sleep(500 * time.Millisecond)

				newDB, err := sql.Open("mysql", tp.config.DBConnectionString)
				if err != nil {
					log.Printf("Failed to reconnect to database: %v", err)
					continue
				}

				// Configure the new connection pool
				newDB.SetMaxOpenConns(15)
				newDB.SetMaxIdleConns(5)
				newDB.SetConnMaxLifetime(1 * time.Minute)
				newDB.SetConnMaxIdleTime(30 * time.Second)

				// Test the new connection
				if err := newDB.Ping(); err != nil {
					log.Printf("Failed to ping database after reconnect: %v", err)
					newDB.Close()
					continue
				}

				// Replace the old connection with the new one
				oldDB := tp.db
				tp.db = newDB
				oldDB.Close()

				log.Println("Successfully reconnected to database")
			}
		}
	}
}

// createTaskPoolTableIfNotExists creates the task_pool table if it doesn't exist
func createTaskPoolTableIfNotExists(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS task_pool (
	  -- Core identification
	  id VARCHAR(255) PRIMARY KEY,
	  idempotency_key VARCHAR(255) NULL,
	  -- Task classification
	  type VARCHAR(255) NOT NULL,
	  priority ENUM ('low', 'medium', 'high', 'critical') NOT NULL DEFAULT 'medium',
	  -- Task content
	  payload JSON NOT NULL,
	  -- Status management
	  status ENUM (
		'pending',
		'processing',
		'completed',
		'failed',
		'cancelled',
		'scheduled'
	  ) NOT NULL DEFAULT 'pending',
	  locked_until DATETIME (6) NULL,
	  -- Retry management
	  retry_count INT UNSIGNED NOT NULL DEFAULT 0,
	  max_retry_count INT UNSIGNED NOT NULL DEFAULT 5,
	  last_error TEXT NULL,
	  -- Scheduling
	  process_after DATETIME (6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	  -- Workflow management
	  correlation_id VARCHAR(255) NULL,
	  -- Audit
	  created_at DATETIME (6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	  updated_at DATETIME (6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
	  -- Constraints
	  UNIQUE KEY uk_idempotency_key (idempotency_key),
	  -- Indexes
	  INDEX idx_status_process_after_priority (status, process_after, priority),
	  INDEX idx_correlation_id (correlation_id),
	  INDEX idx_type (type),
	  INDEX idx_locked_until (locked_until)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`

	_, err := db.Exec(query)
	return err
}

// Start begins the task processing
func (tp *TaskProcessor) Start() {
	log.Println("Starting task processor with max concurrent tasks:", tp.config.MaxConcurrent)

	// Start the main processing loop
	tp.wg.Add(1)
	go tp.processLoop()

	// Start the result processor
	tp.wg.Add(1)
	go tp.resultProcessor()
}

// processLoop is the main processing loop that fetches and processes tasks
func (tp *TaskProcessor) processLoop() {
	defer tp.wg.Done()

	log.Println("Task processor started")

	ticker := time.NewTicker(tp.config.PollInterval)
	defer ticker.Stop()

	// Health check ticker runs every minute
	healthCheckTicker := time.NewTicker(1 * time.Minute)
	defer healthCheckTicker.Stop()

	// Track consecutive failures to implement backoff
	consecutiveFailures := 0

	for {
		select {
		case <-tp.stop:
			log.Println("Task processor stopping")
			return
		case <-healthCheckTicker.C:
			// Perform a health check on the database connection
			if err := tp.db.Ping(); err != nil {
				log.Printf("Database health check failed: %v", err)
			} else {
				log.Println("Database health check passed")
			}
		case <-ticker.C:
			// Check if we're in shutdown mode
			if tp.isShutdownMode() {
				continue // Don't fetch new tasks during shutdown
			}

			// Check how many more tasks we can process
			inFlightCount := tp.getInFlightTaskCount()

			// Check available capacity in the results buffer
			resultsBufferAvailable := cap(tp.resultsChan) - len(tp.resultsChan)

			// Only fetch more tasks if we're below capacity AND there's room in the results buffer
			capacityThreshold := tp.config.MaxConcurrent * 8 / 10
			if inFlightCount < capacityThreshold && resultsBufferAvailable > 0 {
				// Limit batch size based on both worker capacity and results buffer availability
				// This ensures we never overload the results buffer
				batchSize := tp.config.MaxConcurrent - inFlightCount
				if batchSize > resultsBufferAvailable {
					batchSize = resultsBufferAvailable
				}

				tasks, err := tp.fetchTasks(batchSize)
				if err != nil {
					log.Printf("Error fetching tasks: %v", err)

					// Implement exponential backoff for consecutive failures
					consecutiveFailures++
					if consecutiveFailures > 1 {
						backoffDuration := time.Duration(math.Min(30, math.Pow(2, float64(consecutiveFailures-1)))) * time.Second
						log.Printf("Backing off for %v after %d consecutive failures", backoffDuration, consecutiveFailures)
						time.Sleep(backoffDuration)
					}
					continue
				}

				// Reset failure counter on success
				consecutiveFailures = 0

				if len(tasks) > 0 {
					log.Printf("Fetched %d tasks, now processing", len(tasks))

					// Process the new tasks
					for _, task := range tasks {
						tp.processTask(task)
					}
				}
			} else if len(tp.resultsChan) > cap(tp.resultsChan)/2 {
				// If results buffer is more than half full, log a warning
				log.Printf("Results buffer filling up: %d/%d. Waiting for processing to catch up.",
					len(tp.resultsChan), cap(tp.resultsChan))
			}
		}
	}
}

// fetchTasks fetches a batch of tasks from the database using FOR UPDATE SKIP LOCKED
func (tp *TaskProcessor) fetchTasks(taskFetchLimit int) ([]*Task, error) {
	// Check connection before starting transaction
	if err := tp.db.Ping(); err != nil {
		log.Printf("Database connection check failed: %v", err)
		return nil, fmt.Errorf("database connection check failed: %w", err)
	}

	log.Printf("Fetching up to %d tasks", taskFetchLimit)

	// Start a transaction with a context timeout to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := tp.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Select and lock eligible tasks in a single operation
	// FOR UPDATE SKIP LOCKED ensures we only get tasks that aren't locked by other processes
	query := `
		SELECT id, idempotency_key, type, priority, payload, status, 
		       locked_until, retry_count, max_retry_count, last_error, 
		       process_after, correlation_id, created_at, updated_at
		FROM task_pool
		WHERE status = 'pending'
		  AND process_after <= NOW()
		  AND (locked_until IS NULL OR locked_until <= NOW())
		ORDER BY priority DESC, process_after ASC
		LIMIT ?
		FOR UPDATE SKIP LOCKED
	`

	// Use context with timeout for the query
	rows, err := tx.QueryContext(ctx, query, taskFetchLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to query tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*Task
	var taskIDs []string
	lockedUntil := time.Now().Add(tp.config.LockDuration)

	for rows.Next() {
		var task Task
		err := rows.Scan(
			&task.ID, &task.IdempotencyKey, &task.Type, &task.Priority, &task.Payload,
			&task.Status, &task.LockedUntil, &task.RetryCount, &task.MaxRetryCount,
			&task.LastError, &task.ProcessAfter, &task.CorrelationID, &task.CreatedAt,
			&task.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan task: %w", err)
		}

		tasks = append(tasks, &task)
		taskIDs = append(taskIDs, task.ID)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tasks: %w", err)
	}

	if len(tasks) == 0 {
		// No tasks to process, commit the transaction to release locks
		if err := tx.Commit(); err != nil {
			log.Printf("Warning: failed to commit empty transaction: %v", err)
		}
		return nil, nil
	}

	// Update all selected tasks to 'processing' status in a single batch operation
	if len(taskIDs) > 0 {
		placeholders := make([]string, len(taskIDs))
		args := make([]interface{}, len(taskIDs)+1)
		args[0] = lockedUntil

		for i, id := range taskIDs {
			placeholders[i] = "?"
			args[i+1] = id
		}

		updateQuery := fmt.Sprintf(
			"UPDATE task_pool SET status = 'processing', locked_until = ? WHERE id IN (%s)",
			strings.Join(placeholders, ","),
		)

		// Use context with timeout for the update
		_, err := tx.ExecContext(ctx, updateQuery, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to update tasks to processing status: %w", err)
		}
	}

	// Commit the transaction with a timeout
	_, commitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer commitCancel()

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	if len(tasks) > 0 {
		log.Printf("Successfully locked %d/%d tasks", len(tasks), len(taskIDs))
	}

	return tasks, nil
}

// Stop gracefully stops the task processor
func (tp *TaskProcessor) Stop() {
	log.Println("Stopping task processor...")

	// Signal all components to stop adding new tasks
	close(tp.stop)
	tp.batchTicker.Stop()

	// Give in-flight tasks some time to complete
	log.Println("Waiting for in-flight tasks to complete (max 30 seconds)...")

	// Create a timeout channel
	timeout := time.After(30 * time.Second)

	// Create a ticker to check and report progress
	progressTicker := time.NewTicker(5 * time.Second)
	defer progressTicker.Stop()

	// Create a more frequent ticker for debugging in-flight tasks
	debugTicker := time.NewTicker(1 * time.Second)
	defer debugTicker.Stop()

	// Create a ticker to actively process results during shutdown
	processingTicker := time.NewTicker(500 * time.Millisecond)
	defer processingTicker.Stop()

	// Wait for tasks to complete or timeout
	waitComplete := false
	for !waitComplete {
		// Process any results in the channel to help clear the backlog
		processedCount := 0
		for len(tp.resultsChan) > 0 && processedCount < 50 {
			select {
			case result, ok := <-tp.resultsChan:
				if !ok {
					break
				}

				// Remove from in-flight tasks
				tp.tasksMutex.Lock()
				delete(tp.inFlightTasks, result.Task.ID)
				tp.tasksMutex.Unlock()

				// Update task status
				if err := tp.updateTaskStatus(result.Task, result.Error); err != nil {
					log.Printf("Error updating task status during shutdown: %v", err)
				} else {
					log.Printf("Processed task %s during shutdown drain", result.Task.ID)
				}

				processedCount++
			default:
				break
			}
		}

		if processedCount > 0 {
			log.Printf("Processed %d results during shutdown wait", processedCount)
		}

		tp.tasksMutex.RLock()
		remainingTasks := len(tp.inFlightTasks)

		// Debug: log task IDs if there are stuck tasks
		if remainingTasks > 0 && remainingTasks < 10 {
			taskIDs := make([]string, 0, remainingTasks)
			for id := range tp.inFlightTasks {
				taskIDs = append(taskIDs, id)
			}
			log.Printf("Remaining tasks: %v", taskIDs)
		}

		tp.tasksMutex.RUnlock()

		if remainingTasks == 0 {
			log.Println("All in-flight tasks completed successfully")
			waitComplete = true
			break
		}

		select {
		case <-timeout:
			log.Printf("Shutdown timeout reached with %d tasks still in flight", remainingTasks)

			// Check results queue one more time
			queuedResults := len(tp.resultsChan)
			if queuedResults > 0 {
				log.Printf("Results queue still has %d pending results at timeout", queuedResults)

				// Try to drain a few more results
				for i := 0; i < 10 && len(tp.resultsChan) > 0; i++ {
					select {
					case result, ok := <-tp.resultsChan:
						if !ok {
							break
						}

						// Remove from in-flight tasks
						tp.tasksMutex.Lock()
						delete(tp.inFlightTasks, result.Task.ID)
						tp.tasksMutex.Unlock()

						// Update task status
						tp.updateTaskStatus(result.Task, result.Error)
					default:
						break
					}
				}
			}

			// Mark remaining tasks as pending for future processing
			tp.resetRemainingTasks()
			waitComplete = true
			break
		case <-progressTicker.C:
			log.Printf("Shutdown in progress: %d tasks still in flight", remainingTasks)
			continue
		case <-debugTicker.C:
			// Check if the results channel is backed up
			queuedResults := len(tp.resultsChan)
			if queuedResults > 0 {
				log.Printf("Results queue has %d pending results", queuedResults)
			}
			continue
		case <-processingTicker.C:
			// Actively try to process results during shutdown
			continue
		default:
			// Check every 50ms
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Process any remaining results in the channel
	log.Println("Final processing of any remaining results in the channel...")
	remainingCount := 0

	// Try to drain the results channel with a timeout
	drainTimeout := time.After(5 * time.Second)
drainLoop:
	for {
		select {
		case result, ok := <-tp.resultsChan:
			if !ok {
				// Channel closed or empty
				break drainLoop
			}

			// Remove from in-flight tasks
			tp.tasksMutex.Lock()
			delete(tp.inFlightTasks, result.Task.ID)
			tp.tasksMutex.Unlock()

			// Update task status directly
			if err := tp.updateTaskStatus(result.Task, result.Error); err != nil {
				log.Printf("Error updating task status during final drain: %v", err)
			}

			remainingCount++
		case <-drainTimeout:
			log.Printf("Final drain timeout reached after processing %d results", remainingCount)
			break drainLoop
		default:
			// If channel is empty, we're done
			if len(tp.resultsChan) == 0 {
				break drainLoop
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	if remainingCount > 0 {
		log.Printf("Processed %d remaining results during final drain", remainingCount)
	}

	// Wait for all goroutines to finish
	log.Println("Waiting for all goroutines to complete...")

	// Use a timeout for waiting on goroutines
	wgDone := make(chan struct{})
	go func() {
		tp.wg.Wait()
		close(wgDone)
	}()

	select {
	case <-wgDone:
		log.Println("All goroutines completed successfully")
	case <-time.After(5 * time.Second):
		log.Println("Timed out waiting for goroutines to complete")
	}

	// Close the database connection
	tp.db.Close()
	log.Println("Task processor stopped")
}

// processTask processes a single task
func (tp *TaskProcessor) processTask(task *Task) {
	// Add to in-flight tasks
	tp.tasksMutex.Lock()
	tp.inFlightTasks[task.ID] = &InFlightTask{
		Task:      task,
		StartTime: time.Now(),
	}
	tp.tasksMutex.Unlock()

	// Process the task in a goroutine
	tp.wg.Add(1) // Track this goroutine in the WaitGroup
	go func(t *Task) {
		defer tp.wg.Done() // Ensure the WaitGroup is decremented when done

		// Execute the task
		err := tp.executeTask(t)

		// Send the result to the results channel
		select {
		case tp.resultsChan <- TaskResult{Task: t, Error: err}:
			// Result successfully sent to channel
			if err == nil {
				log.Printf("Task %s processed successfully, result queued", t.ID)
			} else {
				log.Printf("Task %s failed, result queued: %v", t.ID, err)
			}
		case <-tp.stop:
			// We're stopping, update directly
			tp.tasksMutex.Lock()
			delete(tp.inFlightTasks, t.ID)
			tp.tasksMutex.Unlock()

			if err := tp.updateTaskStatus(t, err); err != nil {
				log.Printf("Error updating task status during shutdown: %v", err)
			}
		}
	}(task)
}

// resultProcessor processes completed task results
func (tp *TaskProcessor) resultProcessor() {
	defer tp.wg.Done()

	log.Println("Result processor started")

	// Create batches of results to update
	var pendingResults []TaskResult

	// Process results more aggressively during shutdown
	shutdownTicker := time.NewTicker(100 * time.Millisecond)
	defer shutdownTicker.Stop()

	for {
		// Check if we're in shutdown mode to process results more frequently
		isShutdown := tp.isShutdownMode()

		// During shutdown, process results more frequently
		if isShutdown && len(pendingResults) > 0 {
			tp.processBatchResults(pendingResults)
			pendingResults = nil
		}

		select {
		case <-tp.stop:
			log.Println("Result processor stopping")
			// Process any remaining results
			if len(pendingResults) > 0 {
				tp.processBatchResults(pendingResults)
			}

			// During shutdown, drain any remaining results from the channel
			// This ensures we process all completed tasks before exiting
			log.Println("Draining remaining results from channel...")
			drainTimeout := time.After(5 * time.Second)
			drainedCount := 0
		drainLoop:
			for {
				select {
				case result, ok := <-tp.resultsChan:
					if !ok {
						// Channel closed
						break drainLoop
					}

					// Remove from in-flight tasks
					tp.tasksMutex.Lock()
					delete(tp.inFlightTasks, result.Task.ID)
					tp.tasksMutex.Unlock()

					drainedCount++

					// Update the task status directly
					if err := tp.updateTaskStatus(result.Task, result.Error); err != nil {
						log.Printf("Error updating drained task %s: %v", result.Task.ID, err)
					} else {
						log.Printf("Drained and processed task %s during shutdown", result.Task.ID)
					}
				case <-drainTimeout:
					log.Printf("Result drain timeout reached after processing %d results", drainedCount)
					break drainLoop
				default:
					// If no more results immediately available, we're done
					if len(tp.resultsChan) == 0 {
						if drainedCount > 0 {
							log.Printf("Finished draining %d results from channel", drainedCount)
						}
						break drainLoop
					}
					time.Sleep(10 * time.Millisecond)
				}
			}

			return
		case result := <-tp.resultsChan:
			// Remove from in-flight tasks immediately
			tp.tasksMutex.Lock()
			delete(tp.inFlightTasks, result.Task.ID)
			tp.tasksMutex.Unlock()

			// Log successful task completion
			if result.Error == nil {
				log.Printf("Task %s processed successfully", result.Task.ID)
			} else {
				log.Printf("Task %s failed: %v", result.Task.ID, result.Error)
			}

			// Add to pending results
			pendingResults = append(pendingResults, result)

			// Process batch if we have enough results
			if len(pendingResults) >= 100 { // Reduced batch size for more frequent processing
				tp.processBatchResults(pendingResults)
				pendingResults = nil // Reset after processing
			}
		case <-tp.batchTicker.C:
			// Process any pending results on ticker
			if len(pendingResults) > 0 {
				tp.processBatchResults(pendingResults)
				pendingResults = nil // Reset after processing
			}

			// During shutdown, log in-flight task count
			if isShutdown {
				count := tp.getInFlightTaskCount()

				if count > 0 {
					log.Printf("Current in-flight task count: %d", count)
				}
			}
		case <-shutdownTicker.C:
			// During shutdown, process results more frequently
			if isShutdown && len(pendingResults) > 0 {
				tp.processBatchResults(pendingResults)
				pendingResults = nil
			}
		}
	}
}

// processBatchResults processes a batch of task results
func (tp *TaskProcessor) processBatchResults(results []TaskResult) {
	if len(results) == 0 {
		return
	}

	log.Printf("Processing batch of %d task results", len(results))

	// Group tasks by status for batch updates
	completedTasks := make([]string, 0)
	failedTasks := make(map[string]string) // task ID -> error message
	retryTasks := make(map[string]struct {
		retryCount   int
		processAfter time.Time
		errorMsg     string
	})

	// Categorize tasks
	for _, result := range results {
		task := result.Task
		if result.Error == nil {
			// Task completed successfully
			completedTasks = append(completedTasks, task.ID)
		} else {
			// Task failed
			task.RetryCount++
			if task.RetryCount >= task.MaxRetryCount {
				// Max retries reached, mark as failed
				failedTasks[task.ID] = result.Error.Error()
			} else {
				// Schedule for retry with backoff
				backoff := time.Duration(math.Pow(2, float64(task.RetryCount))) * time.Second
				processAfter := time.Now().Add(backoff)

				retryTasks[task.ID] = struct {
					retryCount   int
					processAfter time.Time
					errorMsg     string
				}{
					retryCount:   task.RetryCount,
					processAfter: processAfter,
					errorMsg:     result.Error.Error(),
				}
			}
		}
	}

	// Create a context with timeout for database operations
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start a transaction for batch updates
	tx, err := tp.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("Failed to begin transaction for batch updates: %v", err)
		// Fall back to individual updates
		for _, result := range results {
			if err := tp.updateTaskStatus(result.Task, result.Error); err != nil {
				log.Printf("Error updating task status: %v", err)
			}
		}
		return
	}

	// Update completed tasks in batch if any
	if len(completedTasks) > 0 {
		placeholders := make([]string, len(completedTasks))
		args := make([]interface{}, len(completedTasks))

		for i, id := range completedTasks {
			placeholders[i] = "?"
			args[i] = id
		}

		updateQuery := fmt.Sprintf(
			"UPDATE task_pool SET status = 'completed', locked_until = NULL WHERE id IN (%s)",
			strings.Join(placeholders, ","),
		)

		_, err := tx.ExecContext(ctx, updateQuery, args...)
		if err != nil {
			tx.Rollback()
			log.Printf("Failed to update completed tasks: %v", err)
			// Fall back to individual updates
			for _, id := range completedTasks {
				for _, result := range results {
					if result.Task.ID == id {
						tp.updateTaskStatus(result.Task, nil)
						break
					}
				}
			}
		} else {
			log.Printf("Marked %d tasks as completed", len(completedTasks))
		}
	}

	// Update failed tasks in batch if any
	for id, errMsg := range failedTasks {
		_, err := tx.ExecContext(ctx,
			"UPDATE task_pool SET status = 'failed', retry_count = retry_count + 1, locked_until = NULL, last_error = ? WHERE id = ?",
			errMsg, id,
		)
		if err != nil {
			log.Printf("Failed to update failed task %s: %v", id, err)
		}
	}

	// Update retry tasks in batch if any
	for id, info := range retryTasks {
		_, err := tx.ExecContext(ctx,
			"UPDATE task_pool SET status = 'pending', retry_count = ?, process_after = ?, locked_until = NULL, last_error = ? WHERE id = ?",
			info.retryCount, info.processAfter, info.errorMsg, id,
		)
		if err != nil {
			log.Printf("Failed to update retry task %s: %v", id, err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		log.Printf("Failed to commit batch updates: %v", err)
		// Fall back to individual updates
		for _, result := range results {
			if err := tp.updateTaskStatus(result.Task, result.Error); err != nil {
				log.Printf("Error updating task status: %v", err)
			}
		}
	}
}

// updateTaskStatus updates the task status after processing
func (tp *TaskProcessor) updateTaskStatus(task *Task, taskErr error) error {
	// Create a context with timeout for database operations
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check connection before starting
	if err := tp.db.PingContext(ctx); err != nil {
		log.Printf("Database connection check failed before updating task status: %v", err)
		return fmt.Errorf("database connection check failed: %w", err)
	}

	var updateErr error
	for retries := 0; retries < 5; retries++ {
		if taskErr == nil {
			// Task succeeded - direct update without transaction
			_, updateErr = tp.db.ExecContext(ctx,
				"UPDATE task_pool SET status = 'completed', locked_until = NULL WHERE id = ?",
				task.ID,
			)
			if updateErr == nil {
				log.Printf("Marked task %s as completed", task.ID)
				return nil
			}
		} else {
			// Task failed - direct update without transaction
			task.RetryCount++

			if task.RetryCount >= task.MaxRetryCount {
				// Max retries reached, mark as failed
				_, updateErr = tp.db.ExecContext(ctx,
					"UPDATE task_pool SET status = 'failed', retry_count = ?, locked_until = NULL, last_error = ? WHERE id = ?",
					task.RetryCount, taskErr.Error(), task.ID,
				)
				if updateErr == nil {
					log.Printf("Task %s failed permanently after %d retries: %s", task.ID, task.RetryCount, taskErr.Error())
					return nil
				}
			} else {
				// Calculate exponential backoff
				backoff := time.Duration(math.Pow(2, float64(task.RetryCount))) * time.Second
				processAfter := time.Now().Add(backoff)

				_, updateErr = tp.db.ExecContext(ctx,
					"UPDATE task_pool SET status = 'pending', retry_count = ?, process_after = ?, locked_until = NULL, last_error = ? WHERE id = ?",
					task.RetryCount, processAfter, taskErr.Error(), task.ID,
				)
				if updateErr == nil {
					log.Printf("Task %s failed, scheduled retry %d/%d after %s: %s",
						task.ID, task.RetryCount, task.MaxRetryCount, backoff, taskErr.Error())
					return nil
				}
			}
		}

		// If we get here, the update failed
		log.Printf("Failed to update task status (attempt %d/5): %v", retries+1, updateErr)

		// Check if it's a connection error
		if strings.Contains(updateErr.Error(), "connection") ||
			strings.Contains(updateErr.Error(), "timeout") ||
			strings.Contains(updateErr.Error(), "broken pipe") {
			log.Printf("Detected connection issue, waiting before retry")
			time.Sleep(time.Duration(retries+1) * 500 * time.Millisecond)

			// Force a ping to check/reset connection
			pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
			if pingErr := tp.db.PingContext(pingCtx); pingErr != nil {
				log.Printf("Ping failed during update retry: %v", pingErr)

				// Try to reconnect if ping fails
				tp.reconnectDB()
			}
			pingCancel()
		} else {
			// Other error, shorter wait
			time.Sleep(time.Duration(retries+1) * 200 * time.Millisecond)
		}
	}

	// If we get here, all retries failed
	return fmt.Errorf("failed to update task status after multiple retries: %w", updateErr)
}

// executeTask processes a task by calling the API
func (tp *TaskProcessor) executeTask(task *Task) error {
	log.Printf("Processing task %s of type %s (retry %d/%d)",
		task.ID, task.Type, task.RetryCount, task.MaxRetryCount)

	// Construct the URL for the task type
	url := fmt.Sprintf("%s/task/%s", tp.config.APIEndpoint, task.Type)

	// Create a request object that includes task metadata
	type TaskRequest struct {
		ID            string          `json:"id"`
		Type          string          `json:"type"`
		Priority      string          `json:"priority"`
		Payload       json.RawMessage `json:"payload"`
		RetryCount    int             `json:"retry_count"`
		MaxRetryCount int             `json:"max_retry_count"`
		CorrelationID string          `json:"correlation_id,omitempty"`
	}

	// Create the request body
	correlationID := ""
	if task.CorrelationID.Valid {
		correlationID = task.CorrelationID.String
	}

	requestBody := TaskRequest{
		ID:            task.ID,
		Type:          task.Type,
		Priority:      task.Priority,
		Payload:       task.Payload,
		RetryCount:    task.RetryCount,
		MaxRetryCount: task.MaxRetryCount,
		CorrelationID: correlationID,
	}

	// Marshal the request to JSON
	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("failed to marshal task request: %w", err)
	}

	// Create client with timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Create request with the JSON body
	req, err := http.NewRequest("POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Add task ID as header and set content type
	req.Header.Set("X-Task-ID", task.ID)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("API call failed: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode >= 400 {
		// Try to read error message from response body
		errorBody, _ := io.ReadAll(resp.Body)
		if len(errorBody) > 0 {
			return fmt.Errorf("API returned error status: %d - %s", resp.StatusCode, string(errorBody))
		}
		return fmt.Errorf("API returned error status: %d", resp.StatusCode)
	}

	log.Printf("Task %s processed successfully", task.ID)
	return nil
}

// markTaskCompleted marks a task as completed
func (tp *TaskProcessor) markTaskCompleted(tx *sql.Tx, task *Task) error {
	_, err := tx.Exec(
		"UPDATE task_pool SET status = 'completed', locked_until = NULL WHERE id = ?",
		task.ID,
	)
	if err != nil {
		return err
	}

	log.Printf("Marked task %s as completed", task.ID)
	return nil
}

// markTaskFailed marks a task as failed and handles retry logic
func (tp *TaskProcessor) markTaskFailed(tx *sql.Tx, task *Task, errMsg string) error {
	task.RetryCount++

	if task.RetryCount >= task.MaxRetryCount {
		// Max retries reached, mark as failed
		_, err := tx.Exec(
			"UPDATE task_pool SET status = 'failed', retry_count = ?, locked_until = NULL, last_error = ? WHERE id = ?",
			task.RetryCount, errMsg, task.ID,
		)
		if err != nil {
			return err
		}
		log.Printf("Task %s failed permanently after %d retries: %s", task.ID, task.RetryCount, errMsg)
	} else {
		// Calculate exponential backoff
		backoff := time.Duration(math.Pow(2, float64(task.RetryCount))) * time.Second
		processAfter := time.Now().Add(backoff)

		_, err := tx.Exec(
			"UPDATE task_pool SET status = 'pending', retry_count = ?, process_after = ?, locked_until = NULL, last_error = ? WHERE id = ?",
			task.RetryCount, processAfter, errMsg, task.ID,
		)
		if err != nil {
			return err
		}
		log.Printf("Task %s failed, scheduled retry %d/%d after %s: %s",
			task.ID, task.RetryCount, task.MaxRetryCount, backoff, errMsg)
	}

	return nil
}

// resetRemainingTasks resets any in-flight tasks to pending status during shutdown
func (tp *TaskProcessor) resetRemainingTasks() {
	tp.tasksMutex.Lock()
	remainingTasks := make(map[string]*InFlightTask)
	for id, task := range tp.inFlightTasks {
		remainingTasks[id] = task
	}
	tp.tasksMutex.Unlock()

	if len(remainingTasks) == 0 {
		return
	}

	log.Printf("Resetting %d in-flight tasks to pending status", len(remainingTasks))

	// Create a batch update for all remaining tasks
	taskIDs := make([]string, 0, len(remainingTasks))
	for id := range remainingTasks {
		taskIDs = append(taskIDs, id)
	}

	// Use batches of 100 to avoid too large queries
	batchSize := 100
	for i := 0; i < len(taskIDs); i += batchSize {
		end := i + batchSize
		if end > len(taskIDs) {
			end = len(taskIDs)
		}

		batch := taskIDs[i:end]
		placeholders := make([]string, len(batch))
		args := make([]interface{}, len(batch))

		for j, id := range batch {
			placeholders[j] = "?"
			args[j] = id
		}

		query := fmt.Sprintf(
			"UPDATE task_pool SET status = 'pending', locked_until = NULL WHERE id IN (%s)",
			strings.Join(placeholders, ","),
		)

		_, err := tp.db.Exec(query, args...)
		if err != nil {
			log.Printf("Error resetting tasks during shutdown: %v", err)

			// Fall back to individual updates if batch fails
			for _, id := range batch {
				_, err := tp.db.Exec(
					"UPDATE task_pool SET status = 'pending', locked_until = NULL WHERE id = ?",
					id,
				)
				if err != nil {
					log.Printf("Error resetting task %s: %v", id, err)
				}
			}
		}
	}

	// Clear the in-flight tasks map after resetting them in the database
	tp.tasksMutex.Lock()
	tp.inFlightTasks = make(map[string]*InFlightTask)
	tp.tasksMutex.Unlock()

	log.Printf("Successfully reset in-flight tasks to pending status")
}

// reconnectDB attempts to reconnect to the database
func (tp *TaskProcessor) reconnectDB() {
	log.Println("Attempting to reconnect to database...")

	// Close the existing connection
	tp.db.Close()

	// Wait a moment before reconnecting
	time.Sleep(500 * time.Millisecond)

	// Try to reconnect
	newDB, err := sql.Open("mysql", tp.config.DBConnectionString)
	if err != nil {
		log.Printf("Failed to reconnect to database: %v", err)
		return
	}

	// Configure the new connection
	newDB.SetMaxOpenConns(15)
	newDB.SetMaxIdleConns(5)
	newDB.SetConnMaxLifetime(1 * time.Minute)
	newDB.SetConnMaxIdleTime(30 * time.Second)

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := newDB.PingContext(ctx); err != nil {
		log.Printf("Failed to ping database after reconnect: %v", err)
		newDB.Close()
		return
	}

	// Replace the old connection
	tp.db = newDB
	log.Println("Successfully reconnected to database")
}

func main() {
	// Configuration
	config := Config{
		// Enhanced connection parameters to prevent "busy buffer" errors
		DBConnectionString: "taskuser:taskpassword@tcp(localhost:3306)/taskdb?parseTime=true&timeout=5s&readTimeout=5s&writeTimeout=5s&clientFoundRows=true&maxAllowedPacket=4194304&interpolateParams=true",
		LockDuration:       1 * time.Minute,
		PollInterval:       1 * time.Second,
		APIEndpoint:        "http://localhost:3000",
		MaxConcurrent:      200, // Maximum number of concurrent tasks
	}

	// Override from environment variables if present
	if dbConn := os.Getenv("DB_CONNECTION_STRING"); dbConn != "" {
		config.DBConnectionString = dbConn
	}

	// Create and start the task processor
	processor, err := NewTaskProcessor(config)
	if err != nil {
		log.Fatalf("Failed to create task processor: %v", err)
	}

	// Start the processor
	processor.Start()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Track if we're already in shutdown mode
	var shutdownStarted bool
	var shutdownTime time.Time

	// Wait for termination signal
	for {
		sig := <-sigChan

		if !shutdownStarted {
			// First signal - start graceful shutdown
			log.Printf("Received signal %v, starting graceful shutdown", sig)
			shutdownStarted = true
			shutdownTime = time.Now()

			// Enter shutdown mode before stopping
			processor.setShutdownMode(true)
			log.Println("Entered shutdown mode - no new tasks will be fetched")

			// Start graceful shutdown in a goroutine
			go func() {
				processor.Stop()
				os.Exit(0)
			}()
		} else {
			// Second signal or signal during shutdown - force exit
			timeSinceShutdown := time.Since(shutdownTime)
			log.Printf("Received second signal %v after %v, forcing immediate shutdown", sig, timeSinceShutdown.Round(time.Second))

			// Force exit with a non-zero status code
			os.Exit(1)
		}
	}
}
