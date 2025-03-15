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

// BackoffStrategy defines how to calculate retry delays
type BackoffStrategy struct {
	// Delays in seconds for each retry attempt (1-based index)
	// e.g. [5, 30, 300, 1800] means:
	// 1st retry: 5 seconds
	// 2nd retry: 30 seconds
	// 3rd retry: 5 minutes (300 seconds)
	// 4th retry: 30 minutes (1800 seconds)
	// 5th retry: 30 minutes (1800 seconds) - uses the last value if retries exceed array length
	Delays []int
}

// DefaultBackoffStrategy provides a sensible default backoff strategy
var DefaultBackoffStrategy = BackoffStrategy{
	Delays: []int{
		5,    // 1st retry: 5 seconds
		30,   // 2nd retry: 30 seconds
		300,  // 3rd retry: 5 minutes
		1800, // 4th retry: 30 minutes
		7200, // 5th retry: 2 hours
	},
}

// calculateBackoff determines how long to wait before the next retry
func (bs BackoffStrategy) calculateBackoff(retryCount int) time.Duration {
	if retryCount <= 0 {
		return 0
	}

	// Use the last defined delay for any retry count beyond what's defined
	index := retryCount - 1
	if index >= len(bs.Delays) {
		index = len(bs.Delays) - 1
	}

	return time.Duration(bs.Delays[index]) * time.Second
}

// Config holds application configuration
type Config struct {
	DBConnectionString string
	LockDuration       time.Duration
	PollInterval       time.Duration
	APIEndpoint        string
	MaxConcurrent      int
	BackoffStrategy    BackoffStrategy // Added backoff strategy to config
	// New configuration parameters for improved buffering
	MaxQueryBatchSize     int     // Maximum number of tasks to fetch in a single query
	TaskBufferSize        int     // Target size of the task buffer
	BufferRefillThreshold float64 // Refill buffer when it falls below this percentage (0.0-1.0)
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
	// New fields for task buffering
	taskBuffer      []*Task
	taskBufferMutex sync.RWMutex
	bufferRefillC   chan struct{} // Signal channel to trigger buffer refill
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
	// If no backoff strategy is provided, use the default
	if len(config.BackoffStrategy.Delays) == 0 {
		config.BackoffStrategy = DefaultBackoffStrategy
	}

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

	// Set default values for new configuration parameters if not provided
	if config.MaxQueryBatchSize <= 0 {
		return nil, fmt.Errorf("MaxQueryBatchSize must be greater than 0")
	}
	if config.TaskBufferSize <= 0 {
		return nil, fmt.Errorf("TaskBufferSize must be greater than 0")
	}
	if config.BufferRefillThreshold <= 0 || config.BufferRefillThreshold >= 1 {
		return nil, fmt.Errorf("BufferRefillThreshold must be between 0 and 1")
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
		taskBuffer:    make([]*Task, 0, config.TaskBufferSize),
		bufferRefillC: make(chan struct{}, 1), // Buffered channel to prevent blocking
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
	log.Printf("Buffer configuration: Size=%d, QueryBatchSize=%d, RefillThreshold=%.1f%%",
		tp.config.TaskBufferSize, tp.config.MaxQueryBatchSize, tp.config.BufferRefillThreshold*100)

	// Start the main processing loop
	tp.wg.Add(1)
	go tp.processLoop()

	// Start the buffer management loop
	tp.wg.Add(1)
	go tp.bufferManagementLoop()

	// Start the result processor
	tp.wg.Add(1)
	go tp.resultProcessor()
}

// processLoop is the main processing loop that processes tasks from the buffer
func (tp *TaskProcessor) processLoop() {
	defer tp.wg.Done()

	log.Println("Task processor started")

	processTicker := time.NewTicker(100 * time.Millisecond) // Process more frequently than we poll
	defer processTicker.Stop()

	// Health check ticker runs every minute
	healthCheckTicker := time.NewTicker(1 * time.Minute)
	defer healthCheckTicker.Stop()

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
				log.Printf("Database health check passed. Buffer: %d/%d, In-flight: %d/%d",
					tp.getBufferSize(), tp.config.TaskBufferSize,
					tp.getInFlightTaskCount(), tp.config.MaxConcurrent)
			}

		case <-processTicker.C:
			// Check if we're in shutdown mode
			if tp.isShutdownMode() {
				continue // Don't fetch new tasks during shutdown
			}

			// Check how many more tasks we can process
			inFlightCount := tp.getInFlightTaskCount()

			// Only process more if we're below capacity
			if inFlightCount < tp.config.MaxConcurrent {
				// Calculate how many tasks we can add
				availableSlots := tp.config.MaxConcurrent - inFlightCount

				// Get tasks from buffer
				tasks := tp.getTasksFromBuffer(availableSlots)
				if len(tasks) == 0 {
					continue
				}

				log.Printf("Processing %d tasks from buffer (in-flight: %d/%d)",
					len(tasks), inFlightCount, tp.config.MaxConcurrent)

				// Process the tasks
				for _, task := range tasks {
					tp.processTask(task)
				}
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
			// Check if the results queue is backed up
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

			// Update task status
			tp.updateTaskStatus(result.Task, result.Error)
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

	// Read response body
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	// Check response status - only 2XX is considered success
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("Task %s received non-2XX status code: %d %s", task.ID, resp.StatusCode, resp.Status)

		// Create a structured error response with all details
		var bodyField interface{} = string(responseBody)

		// Try to parse the response body as JSON if it looks like JSON
		if len(responseBody) > 0 && (responseBody[0] == '{' || responseBody[0] == '[') {
			var jsonBody interface{}
			if err := json.Unmarshal(responseBody, &jsonBody); err == nil {
				bodyField = jsonBody
			}
		}

		errorResponse := struct {
			StatusCode int         `json:"status_code"`
			Body       interface{} `json:"body"`
			URL        string      `json:"url"`
		}{
			StatusCode: resp.StatusCode,
			Body:       bodyField,
			URL:        url,
		}

		// Marshal the error response to JSON
		errorJSON, jsonErr := json.Marshal(errorResponse)
		if jsonErr != nil {
			// If JSON marshaling fails, fall back to a simpler error format
			return fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(responseBody))
		}

		errorStr := string(errorJSON)
		log.Printf("Task %s error details: %s", task.ID, errorStr)
		return fmt.Errorf("%s", errorStr)
	}

	log.Printf("Task %s processed successfully with status code %d", task.ID, resp.StatusCode)
	return nil
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
				log.Printf("Task %s failed with error: %v", result.Task.ID, result.Error)
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
			log.Printf("Marking task %s as completed (no error)", task.ID)
			completedTasks = append(completedTasks, task.ID)
		} else {
			// Task failed
			log.Printf("Marking task %s as failed with error: %v", task.ID, result.Error)
			task.RetryCount++
			if task.RetryCount >= task.MaxRetryCount {
				// Max retries reached, mark as failed
				log.Printf("Task %s reached max retries (%d/%d), marking as permanently failed",
					task.ID, task.RetryCount, task.MaxRetryCount)
				failedTasks[task.ID] = result.Error.Error()
			} else {
				// Schedule for retry with backoff
				backoff := tp.config.BackoffStrategy.calculateBackoff(task.RetryCount)
				processAfter := time.Now().Add(backoff)
				log.Printf("Task %s scheduled for retry %d/%d after %s",
					task.ID, task.RetryCount, task.MaxRetryCount, backoff)

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
		// Fall back to individual updates if batch fails
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

		// Use context with timeout for the update
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
		} else {
			log.Printf("Marked task %s as permanently failed", id)
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
		} else {
			log.Printf("Scheduled task %s for retry attempt %d", id, info.retryCount)
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
		// Calculate backoff using the strategy
		backoff := tp.config.BackoffStrategy.calculateBackoff(task.RetryCount)
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
				// Calculate backoff using the strategy
				backoff := tp.config.BackoffStrategy.calculateBackoff(task.RetryCount)
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

		// If it's a connection error
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
		placeholders := make([]string, 0, len(batch))
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

// getTasksFromBuffer safely gets tasks from the buffer
func (tp *TaskProcessor) getTasksFromBuffer(count int) []*Task {
	tp.taskBufferMutex.Lock()
	defer tp.taskBufferMutex.Unlock()

	if len(tp.taskBuffer) == 0 {
		return nil
	}

	// Take up to 'count' tasks from the buffer
	tasksToProcess := count
	if tasksToProcess > len(tp.taskBuffer) {
		tasksToProcess = len(tp.taskBuffer)
	}

	tasks := tp.taskBuffer[:tasksToProcess]
	tp.taskBuffer = tp.taskBuffer[tasksToProcess:]

	// Signal for buffer refill if needed
	if float64(len(tp.taskBuffer)) < float64(tp.config.TaskBufferSize)*tp.config.BufferRefillThreshold {
		// Non-blocking send to trigger refill
		select {
		case tp.bufferRefillC <- struct{}{}:
			// Signal sent
		default:
			// Channel full, refill already scheduled
		}
	}

	return tasks
}

// addTasksToBuffer safely adds tasks to the buffer
func (tp *TaskProcessor) addTasksToBuffer(tasks []*Task) {
	if len(tasks) == 0 {
		return
	}

	tp.taskBufferMutex.Lock()
	defer tp.taskBufferMutex.Unlock()

	// Calculate how many tasks we can add without exceeding buffer size
	remainingCapacity := tp.config.TaskBufferSize - len(tp.taskBuffer)
	tasksToAdd := len(tasks)

	if tasksToAdd > remainingCapacity {
		tasksToAdd = remainingCapacity
		log.Printf("Warning: Task buffer at capacity, only adding %d/%d tasks",
			tasksToAdd, len(tasks))
	}

	if tasksToAdd > 0 {
		tp.taskBuffer = append(tp.taskBuffer, tasks[:tasksToAdd]...)
	}
}

// getBufferSize safely gets the current buffer size
func (tp *TaskProcessor) getBufferSize() int {
	tp.taskBufferMutex.RLock()
	defer tp.taskBufferMutex.RUnlock()
	return len(tp.taskBuffer)
}

// bufferManagementLoop manages the task buffer
func (tp *TaskProcessor) bufferManagementLoop() {
	defer tp.wg.Done()

	log.Println("Buffer management loop started")

	// Initial buffer fill
	tp.fillBuffer()

	// Regular polling ticker
	pollTicker := time.NewTicker(tp.config.PollInterval)
	defer pollTicker.Stop()

	// Track consecutive failures for backoff
	consecutiveFailures := 0

	for {
		select {
		case <-tp.stop:
			log.Println("Buffer management loop stopping")
			return

		case <-tp.bufferRefillC:
			// Buffer needs refill (triggered by getTasksFromBuffer)
			if !tp.isShutdownMode() {
				tp.fillBuffer()
			}

		case <-pollTicker.C:
			// Regular poll interval check
			if tp.isShutdownMode() {
				continue // Don't fetch new tasks during shutdown
			}

			// Check if buffer needs refilling
			bufferSize := tp.getBufferSize()
			bufferThreshold := int(float64(tp.config.TaskBufferSize) * tp.config.BufferRefillThreshold)

			if bufferSize < bufferThreshold {
				log.Printf("Buffer below threshold (%d/%d), refilling", bufferSize, tp.config.TaskBufferSize)

				// Attempt to fill the buffer
				filled, err := tp.fillBuffer()

				if err != nil {
					log.Printf("Error filling buffer: %v", err)

					// Implement exponential backoff for consecutive failures
					consecutiveFailures++
					if consecutiveFailures > 1 {
						backoffDuration := time.Duration(math.Min(30, math.Pow(2, float64(consecutiveFailures-1)))) * time.Second
						log.Printf("Backing off buffer refill for %v after %d consecutive failures",
							backoffDuration, consecutiveFailures)
						time.Sleep(backoffDuration)

					}
				} else {
					// Reset failure counter on success
					consecutiveFailures = 0

					if filled > 0 {
						log.Printf("Added %d tasks to buffer (now %d/%d)",
							filled, tp.getBufferSize(), tp.config.TaskBufferSize)
					}
				}
			}
		}
	}
}

// fillBuffer fetches tasks from the database to fill the buffer
// Returns the number of tasks added and any error
func (tp *TaskProcessor) fillBuffer() (int, error) {
	// Check if we're in shutdown mode
	if tp.isShutdownMode() {
		return 0, nil
	}

	// Calculate how many tasks we need to fetch
	tp.taskBufferMutex.RLock()
	currentBufferSize := len(tp.taskBuffer)
	spaceAvailable := tp.config.TaskBufferSize - currentBufferSize
	tp.taskBufferMutex.RUnlock()

	if spaceAvailable <= 0 {
		return 0, nil // Buffer is full
	}

	// Limit fetch size to configured batch size
	fetchCount := spaceAvailable
	if fetchCount > tp.config.MaxQueryBatchSize {
		fetchCount = tp.config.MaxQueryBatchSize
	}

	// Fetch tasks
	tasks, err := tp.fetchTasks(fetchCount)
	if err != nil {
		return 0, err
	}

	if len(tasks) > 0 {
		// Add tasks to buffer
		tp.addTasksToBuffer(tasks)
		return len(tasks), nil
	}

	return 0, nil
}

func main() {
	// Configuration
	config := Config{
		// Enhanced connection parameters to prevent "busy buffer" errors
		DBConnectionString:    "taskuser:taskpassword@tcp(localhost:3306)/taskdb?parseTime=true&timeout=5s&readTimeout=5s&writeTimeout=5s&clientFoundRows=true&maxAllowedPacket=4194304&interpolateParams=true",
		LockDuration:          1 * time.Minute,
		PollInterval:          1 * time.Second,
		APIEndpoint:           "http://localhost:3000",
		MaxConcurrent:         50, // Default to 50 concurrent tasks
		BackoffStrategy:       DefaultBackoffStrategy,
		MaxQueryBatchSize:     200, // Fetch 200 tasks per query
		TaskBufferSize:        600, // Keep 600 tasks in buffer (3 full DB trips)
		BufferRefillThreshold: 0.5, // Refill when buffer falls below 50%
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
