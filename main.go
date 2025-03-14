package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
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
	WorkerCount        int
	LockDuration       time.Duration
	PollInterval       time.Duration
	APIEndpoint        string
	TaskBufferSize     int
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
}

// NewTaskProcessor creates a new task processor
func NewTaskProcessor(config Config) (*TaskProcessor, error) {
	db, err := sql.Open("mysql", config.DBConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Configure connection pool - use more conservative settings
	db.SetMaxOpenConns(15)                  // Reduced from 25
	db.SetMaxIdleConns(5)                   // Reduced from 10
	db.SetConnMaxLifetime(1 * time.Minute)  // Reduced from 5 minutes
	db.SetConnMaxIdleTime(30 * time.Second) // Reduced from 5 minutes

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Create the task_pool table if it doesn't exist
	if err := createTaskPoolTableIfNotExists(db); err != nil {
		return nil, fmt.Errorf("failed to create task_pool table: %w", err)
	}

	processor := &TaskProcessor{
		db:            db,
		config:        config,
		stop:          make(chan struct{}),
		inFlightTasks: make(map[string]*InFlightTask),
		resultsChan:   make(chan TaskResult, config.MaxConcurrent),
		batchTicker:   time.NewTicker(1 * time.Second), // Process results every second
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
			// Check how many more tasks we can process
			tp.tasksMutex.RLock()
			inFlightCount := len(tp.inFlightTasks)
			tp.tasksMutex.RUnlock()

			// Only fetch more tasks if we're below 80% capacity
			capacityThreshold := tp.config.MaxConcurrent * 8 / 10
			if inFlightCount < capacityThreshold {
				batchSize := tp.config.MaxConcurrent - inFlightCount
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
			}
		}
	}
}

// fetchTasks fetches a batch of tasks from the database
func (tp *TaskProcessor) fetchTasks(batchSize int) ([]*Task, error) {
	// Use a smaller batch size to reduce transaction time
	if batchSize > 10 {
		batchSize = 10 // Limit batch size to reduce transaction duration
	}

	// Check connection before starting transaction
	if err := tp.db.Ping(); err != nil {
		log.Printf("Database connection check failed: %v", err)
		return nil, fmt.Errorf("database connection check failed: %w", err)
	}

	// Use a simpler approach - fetch IDs first, then lock each task individually
	// This avoids long-running transactions that are more prone to connection issues

	log.Printf("Fetching up to %d tasks", batchSize)

	// Step 1: Get task IDs that are eligible for processing
	query := `
		SELECT id
		FROM task_pool
		WHERE status = 'pending'
		  AND process_after <= NOW()
		  AND (locked_until IS NULL OR locked_until <= NOW())
		ORDER BY priority DESC, process_after ASC
		LIMIT ?
	`

	rows, err := tp.db.Query(query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query task IDs: %w", err)
	}
	defer rows.Close()

	var taskIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan task ID: %w", err)
		}
		taskIDs = append(taskIDs, id)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating task IDs: %w", err)
	}

	if len(taskIDs) == 0 {
		return nil, nil // No tasks to process
	}

	// Step 2: Lock and fetch each task individually
	var tasks []*Task
	lockedUntil := time.Now().Add(tp.config.LockDuration)

	for _, taskID := range taskIDs {
		// Try to lock the task
		result, err := tp.db.Exec(
			"UPDATE task_pool SET status = 'processing', locked_until = ? WHERE id = ? AND status = 'pending'",
			lockedUntil, taskID,
		)
		if err != nil {
			log.Printf("Failed to lock task %s: %v", taskID, err)
			continue // Skip this task and try the next one
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil || rowsAffected == 0 {
			// Task was already taken by another process or error occurred
			continue
		}

		// Fetch the locked task
		var task Task
		err = tp.db.QueryRow(`
			SELECT id, idempotency_key, type, priority, payload, status, 
			       locked_until, retry_count, max_retry_count, last_error, 
			       process_after, correlation_id, created_at, updated_at
			FROM task_pool
			WHERE id = ?
		`, taskID).Scan(
			&task.ID, &task.IdempotencyKey, &task.Type, &task.Priority, &task.Payload,
			&task.Status, &task.LockedUntil, &task.RetryCount, &task.MaxRetryCount,
			&task.LastError, &task.ProcessAfter, &task.CorrelationID, &task.CreatedAt,
			&task.UpdatedAt,
		)
		if err != nil {
			log.Printf("Failed to fetch task %s after locking: %v", taskID, err)
			// Release the lock since we couldn't fetch the task
			tp.db.Exec("UPDATE task_pool SET status = 'pending', locked_until = NULL WHERE id = ?", taskID)
			continue
		}

		tasks = append(tasks, &task)
	}

	if len(tasks) > 0 {
		log.Printf("Successfully locked %d/%d tasks", len(tasks), len(taskIDs))
	}

	return tasks, nil
}

// Stop gracefully stops the task processor
func (tp *TaskProcessor) Stop() {
	log.Println("Stopping task processor...")
	close(tp.stop)
	tp.batchTicker.Stop()
	tp.wg.Wait()
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
	go func(t *Task) {
		// Execute the task
		err := tp.executeTask(t)

		// Send the result to the results channel
		tp.resultsChan <- TaskResult{
			Task:  t,
			Error: err,
		}
	}(task)
}

// resultProcessor processes completed task results
func (tp *TaskProcessor) resultProcessor() {
	defer tp.wg.Done()

	log.Println("Result processor started")

	// Create batches of results to update
	var pendingResults []TaskResult

	for {
		select {
		case <-tp.stop:
			log.Println("Result processor stopping")
			// Process any remaining results
			if len(pendingResults) > 0 {
				tp.processBatchResults(pendingResults)
			}
			return
		case result := <-tp.resultsChan:
			// Remove from in-flight tasks
			tp.tasksMutex.Lock()
			delete(tp.inFlightTasks, result.Task.ID)
			tp.tasksMutex.Unlock()

			// Add to pending results
			pendingResults = append(pendingResults, result)

			// Process batch if we have enough results or on ticker
			if len(pendingResults) >= 50 { // Process in batches of 50
				tp.processBatchResults(pendingResults)
				pendingResults = nil // Reset after processing
			}
		case <-tp.batchTicker.C:
			// Process any pending results on ticker
			if len(pendingResults) > 0 {
				tp.processBatchResults(pendingResults)
				pendingResults = nil // Reset after processing
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

	// Start a transaction for batch updates
	tx, err := tp.db.Begin()
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

		query := fmt.Sprintf(
			"UPDATE task_pool SET status = 'completed', locked_until = NULL WHERE id IN (%s)",
			strings.Join(placeholders, ","),
		)

		_, err := tx.Exec(query, args...)
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
		_, err := tx.Exec(
			"UPDATE task_pool SET status = 'failed', retry_count = retry_count + 1, locked_until = NULL, last_error = ? WHERE id = ?",
			errMsg, id,
		)
		if err != nil {
			log.Printf("Failed to update failed task %s: %v", id, err)
		}
	}

	// Update retry tasks in batch if any
	for id, info := range retryTasks {
		_, err := tx.Exec(
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
	// Check connection before starting
	if err := tp.db.Ping(); err != nil {
		log.Printf("Database connection check failed before updating task status: %v", err)
		return fmt.Errorf("database connection check failed: %w", err)
	}

	// Avoid transactions for simple updates to reduce connection issues
	// Instead, use direct updates with retries

	var updateErr error
	for retries := 0; retries < 5; retries++ {
		if taskErr == nil {
			// Task succeeded - direct update without transaction
			_, updateErr = tp.db.Exec(
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
				_, updateErr = tp.db.Exec(
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

				_, updateErr = tp.db.Exec(
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
		if updateErr.Error() == "driver: bad connection" {
			log.Printf("Detected bad connection, waiting before retry")
			time.Sleep(time.Duration(retries+1) * 500 * time.Millisecond)

			// Force a ping to check/reset connection
			if pingErr := tp.db.Ping(); pingErr != nil {
				log.Printf("Ping failed during update retry: %v", pingErr)
			}
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

	// Simulate API call to process the task
	url := fmt.Sprintf("%s/task/%s", tp.config.APIEndpoint, task.Type)

	// Create a client with timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Create request with task payload
	req, err := http.NewRequest("POST", url, bytes.NewReader(task.Payload))
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

func main() {
	// Configuration
	config := Config{
		// Enhanced connection parameters to prevent "busy buffer" errors
		DBConnectionString: "taskuser:taskpassword@tcp(localhost:3306)/taskdb?parseTime=true&timeout=10s&readTimeout=10s&writeTimeout=10s&clientFoundRows=true&maxAllowedPacket=4194304&interpolateParams=true",
		WorkerCount:        10, // Still used for connection pool sizing
		LockDuration:       2 * time.Minute,
		PollInterval:       2 * time.Second,
		APIEndpoint:        "http://localhost:3000",
		TaskBufferSize:     500,  // Used as a guideline for batch sizes
		MaxConcurrent:      1000, // Maximum number of concurrent tasks
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

	// Wait for termination signal
	<-sigChan
	log.Println("Received shutdown signal")

	// Stop the processor
	processor.Stop()
}
