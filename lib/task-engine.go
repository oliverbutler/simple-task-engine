package lib

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"simple-task-engine/lib/metrics"
	"simple-task-engine/lib/store"
	"simple-task-engine/lib/types"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// TaskResult represents the result of a task execution
type TaskResult struct {
	Task  *types.Task
	Error error
}

// InFlightTask represents a task that is currently being processed
type InFlightTask struct {
	Task      *types.Task
	StartTime time.Time
}

// DefaultBackoffStrategy provides a sensible default backoff strategy
var DefaultBackoffStrategy = types.BackoffStrategy{
	Delays: []int{
		5,    // 1st retry: 5 seconds
		30,   // 2nd retry: 30 seconds
		300,  // 3rd retry: 5 minutes
		1800, // 4th retry: 30 minutes
		7200, // 5th retry: 2 hours
	},
}

// TaskProcessor handles task processing
type TaskProcessor struct {
	db              *sql.DB
	taskRepository  store.TaskRepository
	config          types.Config
	wg              sync.WaitGroup
	stop            chan struct{}
	fillBufferMutex sync.Mutex
	isFillingBuffer bool
	inFlightTasks   map[string]*InFlightTask
	tasksMutex      sync.RWMutex
	resultsChan     chan TaskResult
	batchTicker     *time.Ticker
	shutdownMode    bool         // Flag to indicate we're in shutdown mode
	shutdownMutex   sync.RWMutex // Mutex for the shutdown flag
	taskBuffer      []*types.Task
	taskBufferMutex sync.RWMutex
	metrics         *metrics.Metrics
	metricsServer   *http.Server
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

// tryFillBuffer attempts to set the fetching state
// returns true if successful (wasn't already fetching)
// returns false if already fetching
func (tp *TaskProcessor) tryFillBuffer() bool {
	tp.fillBufferMutex.Lock()
	defer tp.fillBufferMutex.Unlock()

	if tp.isFillingBuffer {
		return false
	}

	tp.isFillingBuffer = true
	return true
}

// unsetFillBuffer marks the fetching as complete
func (tp *TaskProcessor) unsetFillBuffer() {
	tp.fillBufferMutex.Lock()
	defer tp.fillBufferMutex.Unlock()
	tp.isFillingBuffer = false
}

// NewTaskProcessor creates a new task processor
func NewTaskProcessor(config types.Config) (*TaskProcessor, error) {
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

	// Create metrics
	metrics := metrics.NewMetrics()

	// Set initial values for capacity metrics
	metrics.BufferCapacity.Set(float64(config.TaskBufferSize))
	metrics.MaxConcurrentTasks.Set(float64(config.MaxConcurrent))
	metrics.ResultsChannelCap.Set(float64(resultsBufferSize))

	// TODO: Consider postgres repository also
	taskRepository := store.NewTaskRepositoryMySQL(db, config.DBTableName, config.LockDuration)

	processor := &TaskProcessor{
		db:             db,
		taskRepository: taskRepository,
		config:         config,
		stop:           make(chan struct{}),
		inFlightTasks:  make(map[string]*InFlightTask),
		resultsChan:    make(chan TaskResult, resultsBufferSize),
		batchTicker:    time.NewTicker(1 * time.Second), // Process results every second
		shutdownMode:   false,
		taskBuffer:     make([]*types.Task, 0, config.TaskBufferSize),
		metrics:        metrics,
	}

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

// Start begins the task processing
func (tp *TaskProcessor) Start() {
	log.Println("Starting task processor with max concurrent tasks:", tp.config.MaxConcurrent)
	log.Printf("Buffer configuration: Size=%d, QueryBatchSize=%d, RefillThreshold=%.1f%%",
		tp.config.TaskBufferSize, tp.config.MaxQueryBatchSize, tp.config.BufferRefillThreshold*100)

	tp.wg.Add(1)
	go tp.taskBufferLoop()

	tp.wg.Add(1)
	go tp.processLoop()

	tp.wg.Add(1)
	go tp.resultProcessor()

	go tp.startMetricsServer(":9090")
	go tp.monitorDatabaseConnection()
	go tp.updateMetrics()
}

// processLoop is the main processing loop that processes tasks from the buffer
func (tp *TaskProcessor) processLoop() {
	defer func() {
		tp.wg.Done()
		log.Println("processLoop > stopped")
	}()
	log.Println("processLoop > started")

	processTicker := time.NewTicker(100 * time.Millisecond) // Process more frequently than we poll
	defer processTicker.Stop()

	// Health check ticker runs every minute
	healthCheckTicker := time.NewTicker(1 * time.Minute)
	defer healthCheckTicker.Stop()

	for {
		select {
		case <-tp.stop:
			log.Println("processLoop > Task processor stopping")
			return

		case <-healthCheckTicker.C:
			// Perform a health check on the database connection
			if err := tp.db.Ping(); err != nil {
				log.Printf("processLoop > Database health check failed: %v", err)
			} else {
				log.Printf("processLoop > Database health check passed. Buffer: %d/%d, In-flight: %d/%d",
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

				log.Printf("processLoop > Processing %d tasks from buffer (in-flight: %d/%d)",
					len(tasks), inFlightCount, tp.config.MaxConcurrent)

				// Process the tasks
				for _, task := range tasks {
					tp.processTask(task)
				}
			}
		}
	}
}

// Stop gracefully stops the task processor
func (tp *TaskProcessor) Stop() {
	log.Println("Stopping task processor...")

	tp.setShutdownMode(true)
	log.Println("Entered shutdown mode - no new tasks will be fetched")

	close(tp.stop)

	log.Println("Waiting for all goroutines to complete...")

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

	if tp.metricsServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := tp.metricsServer.Shutdown(ctx); err != nil {
			log.Printf("Error shutting down metrics server: %v", err)
		}
	}

	tp.db.Close()
	log.Println("Task processor stopped")
}

// executeTask processes a task by calling the API
func (tp *TaskProcessor) executeTask(task *types.Task) error {
	log.Printf("Processing task %s of type %s (retry %d/%d)",
		task.ID, task.Type, task.RetryCount, task.MaxRetryCount)

	// Increment task processing counter by type and priority
	tp.metrics.TasksProcessed.Inc()
	tp.metrics.TasksByType.WithLabelValues(task.Type).Inc()
	tp.metrics.TasksByPriority.WithLabelValues(task.Priority).Inc()

	// Track task processing time
	startTime := time.Now()
	defer func() {
		tp.metrics.TaskProcessingDuration.Observe(time.Since(startTime).Seconds())
	}()

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

	// Track API request metrics
	tp.metrics.APIRequestsTotal.Inc()
	apiStartTime := time.Now()

	// Execute request
	resp, err := client.Do(req)

	// Observe API request duration
	tp.metrics.APIRequestDuration.Observe(time.Since(apiStartTime).Seconds())

	if err != nil {
		tp.metrics.APIRequestErrors.Inc()
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
		var bodyField any = string(responseBody)

		// Try to parse the response body as JSON if it looks like JSON
		if len(responseBody) > 0 && (responseBody[0] == '{' || responseBody[0] == '[') {
			var jsonBody any
			if err := json.Unmarshal(responseBody, &jsonBody); err == nil {
				bodyField = jsonBody
			}
		}

		errorResponse := struct {
			StatusCode int    `json:"status_code"`
			Body       any    `json:"body"`
			URL        string `json:"url"`
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
func (tp *TaskProcessor) processTask(task *types.Task) {
	// Add to in-flight tasks
	tp.tasksMutex.Lock()
	tp.inFlightTasks[task.ID] = &InFlightTask{
		Task:      task,
		StartTime: time.Now(),
	}
	tp.tasksMutex.Unlock()

	// Process the task in a goroutine
	tp.wg.Add(1) // Track this goroutine in the WaitGroup
	go func(t *types.Task) {
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
		}
	}(task)
}

// resultProcessor processes completed task results
func (tp *TaskProcessor) resultProcessor() {
	defer func() {
		tp.wg.Done()
		log.Println("resultProcessor > stopped")
	}()
	log.Println("resultProcessor > started")

	// Create batches of results to update
	var pendingResults []TaskResult

	for {
		select {
		case <-tp.stop:
			log.Println("resultProcessor > starting to stop")

			if len(pendingResults) > 0 {
				log.Printf("resultProcessor > before stopping, processing %d pending results", len(pendingResults))
				tp.processBatchResults(pendingResults)
				pendingResults = nil
			}

			// Wait for any in-flight tasks to complete during shutdown
			// This ensures we don't exit until all tasks have sent their results
			// for {
			// 	inFlightCount := tp.getInFlightTaskCount()
			// 	if inFlightCount == 0 {
			// 		log.Println("resultProcessor > all in-flight tasks completed, exiting")
			// 		return
			// 	}
			//
			// 	log.Printf("resultProcessor > waiting for %d in-flight tasks to complete", inFlightCount)
			//
			// 	// Wait for more results or check again after a short delay
			// 	select {
			// 	case result := <-tp.resultsChan:
			// 		// Process this result immediately
			// 		tp.tasksMutex.Lock()
			// 		delete(tp.inFlightTasks, result.Task.ID)
			// 		tp.tasksMutex.Unlock()
			//
			// 		log.Printf("resultProcessor > shutdown: processing task %s result", result.Task.ID)
			// 		tp.processBatchResults([]TaskResult{result})
			// 	case <-time.After(100 * time.Millisecond):
			// 		// Just check the count again
			// 	}
			// }
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
			if len(pendingResults) >= 100 {
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
				backoff := tp.config.BackoffStrategy.CalculateBackoff(task.RetryCount)
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
		args := make([]any, len(completedTasks))

		for i, id := range completedTasks {
			placeholders[i] = "?"
			args[i] = id
		}

		updateQuery := fmt.Sprintf(
			"UPDATE %s SET status = 'completed', locked_until = NULL WHERE id IN (%s)",
			tp.config.DBTableName,
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
			tp.metrics.TaskStatusUpdates.WithLabelValues("completed").Add(float64(len(completedTasks)))

		}
	}

	// Update failed tasks in batch if any
	for id, errMsg := range failedTasks {

		_, err := tx.ExecContext(ctx,
			fmt.Sprintf("UPDATE %s SET status = 'failed', retry_count = retry_count + 1, locked_until = NULL, last_error = ? WHERE id = ?", tp.config.DBTableName),
			errMsg, id,
		)

		if err != nil {
			log.Printf("Failed to update failed task %s: %v", id, err)
		} else {
			log.Printf("Marked task %s as permanently failed", id)
			tp.metrics.TaskStatusUpdates.WithLabelValues("failed").Inc()

		}
	}

	// Update retry tasks in batch if any
	for id, info := range retryTasks {
		_, err := tx.ExecContext(ctx,
			fmt.Sprintf("UPDATE %s SET status = 'pending', retry_count = ?, process_after = ?, locked_until = NULL, last_error = ? WHERE id = ?", tp.config.DBTableName),
			info.retryCount, info.processAfter, info.errorMsg, id,
		)

		if err != nil {
			log.Printf("Failed to update retry task %s: %v", id, err)
		} else {
			log.Printf("Scheduled task %s for retry attempt %d", id, info.retryCount)
			tp.metrics.TaskStatusUpdates.WithLabelValues("pending").Inc()

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
func (tp *TaskProcessor) updateTaskStatus(task *types.Task, taskErr error) error {
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
			_, updateErr := tp.db.ExecContext(ctx,
				fmt.Sprintf("UPDATE %s SET status = 'completed', locked_until = NULL WHERE id = ?", tp.config.DBTableName),
				task.ID,
			)

			if updateErr == nil {
				log.Printf("Marked task %s as completed", task.ID)
				tp.metrics.TasksSucceeded.Inc()
				tp.metrics.TaskStatusUpdates.WithLabelValues("completed").Inc()
				return nil
			}
		} else {
			// Task failed - direct update without transaction
			task.RetryCount++

			if task.RetryCount >= task.MaxRetryCount {
				// Max retries reached, mark as failed
				_, updateErr := tp.db.ExecContext(ctx,
					fmt.Sprintf("UPDATE %s SET status = 'failed', retry_count = ?, locked_until = NULL, last_error = ? WHERE id = ?", tp.config.DBTableName),
					task.RetryCount, taskErr.Error(), task.ID,
				)

				if updateErr == nil {
					log.Printf("Task %s failed permanently after %d retries: %s", task.ID, task.RetryCount, taskErr.Error())
					tp.metrics.TasksFailed.Inc()
					tp.metrics.TaskStatusUpdates.WithLabelValues("failed").Inc()

					return nil
				}
			} else {
				// Calculate backoff using the strategy
				backoff := tp.config.BackoffStrategy.CalculateBackoff(task.RetryCount)
				processAfter := time.Now().Add(backoff)

				_, updateErr := tp.db.ExecContext(ctx,
					fmt.Sprintf("UPDATE %s SET status = 'pending', retry_count = ?, process_after = ?, locked_until = NULL, last_error = ? WHERE id = ?", tp.config.DBTableName),
					task.RetryCount, processAfter, taskErr.Error(), task.ID,
				)

				if updateErr == nil {
					log.Printf("Task %s failed, scheduled retry %d/%d after %s: %s",
						task.ID, task.RetryCount, task.MaxRetryCount, backoff, taskErr.Error())
					tp.metrics.TasksRetried.Inc()
					tp.metrics.TaskStatusUpdates.WithLabelValues("pending").Inc()

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
		end := min(i+batchSize, len(taskIDs))

		batch := taskIDs[i:end]
		placeholders := make([]string, 0, len(batch))
		args := make([]any, len(batch))

		for j, id := range batch {
			placeholders[j] = "?"
			args[j] = id
		}

		query := fmt.Sprintf(
			fmt.Sprintf("UPDATE %s SET status = 'pending', locked_until = NULL WHERE id IN (%%s)", tp.config.DBTableName),
			strings.Join(placeholders, ","),
		)

		_, err := tp.db.Exec(query, args...)
		if err != nil {
			log.Printf("Error resetting tasks during shutdown: %v", err)

			// Fall back to individual updates if batch fails
			for _, id := range batch {
				_, err := tp.db.Exec(
					fmt.Sprintf("UPDATE %s SET status = 'pending', locked_until = NULL WHERE id = ?", tp.config.DBTableName),
					id,
				)
				if err != nil {
					log.Printf("Error resetting task %s: %v", id, err)
				} else {
					log.Printf("Reset task %s to pending during shutdown", id)
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
func (tp *TaskProcessor) getTasksFromBuffer(count int) []*types.Task {
	tp.taskBufferMutex.Lock()
	defer tp.taskBufferMutex.Unlock()

	if len(tp.taskBuffer) == 0 {
		return nil
	}

	// Take up to 'count' tasks from the buffer
	tasksToProcess := min(count, len(tp.taskBuffer))

	tasks := tp.taskBuffer[:tasksToProcess]
	tp.taskBuffer = tp.taskBuffer[tasksToProcess:]

	return tasks
}

// addTasksToBuffer safely adds tasks to the buffer
func (tp *TaskProcessor) addTasksToBuffer(tasks []*types.Task) {
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

// updateMetrics periodically updates metrics that need polling
func (tp *TaskProcessor) updateMetrics() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-tp.stop:
			return
		case <-ticker.C:
			// Update buffer size
			tp.metrics.BufferSize.Set(float64(tp.getBufferSize()))

			// Update in-flight tasks
			tp.metrics.InFlightTasks.Set(float64(tp.getInFlightTaskCount()))

			// Update results channel size
			tp.metrics.ResultsChannelSize.Set(float64(len(tp.resultsChan)))

		}
	}
}

// startMetricsServer starts an HTTP server to expose Prometheus metrics
func (tp *TaskProcessor) startMetricsServer(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	// Add a simple health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		// Check DB connection
		err := tp.db.Ping()
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write(fmt.Appendf(nil, "Database connection error: %v", err))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Add a debug endpoint with current state
	mux.HandleFunc("/debug/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		status := struct {
			BufferSize      int     `json:"buffer_size"`
			BufferCapacity  int     `json:"buffer_capacity"`
			BufferFillLevel float64 `json:"buffer_fill_level"`
			InFlightTasks   int     `json:"in_flight_tasks"`
			MaxConcurrent   int     `json:"max_concurrent"`
			ResultsQueueLen int     `json:"results_queue_length"`
			ResultsQueueCap int     `json:"results_queue_capacity"`
			ShutdownMode    bool    `json:"shutdown_mode"`
			DBConnections   int     `json:"db_connections"`
		}{
			BufferSize:      tp.getBufferSize(),
			BufferCapacity:  tp.config.TaskBufferSize,
			BufferFillLevel: float64(tp.getBufferSize()) / float64(tp.config.TaskBufferSize),
			InFlightTasks:   tp.getInFlightTaskCount(),
			MaxConcurrent:   tp.config.MaxConcurrent,
			ResultsQueueLen: len(tp.resultsChan),
			ResultsQueueCap: cap(tp.resultsChan),
			ShutdownMode:    tp.isShutdownMode(),
			DBConnections:   tp.db.Stats().OpenConnections,
		}

		json.NewEncoder(w).Encode(status)
	})

	tp.metricsServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	log.Printf("Starting metrics server on %s", addr)
	if err := tp.metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("Metrics server error: %v", err)
	}
}
