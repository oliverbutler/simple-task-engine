package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

// WaitGroupCounter is a wrapper around sync.WaitGroup that tracks the counter
type WaitGroupCounter struct {
	wg      sync.WaitGroup
	counter int64
}

// Add adds delta to the WaitGroup counter
func (wgc *WaitGroupCounter) Add(delta int) {
	atomic.AddInt64(&wgc.counter, int64(delta))
	wgc.wg.Add(delta)
}

// Done decrements the WaitGroup counter
func (wgc *WaitGroupCounter) Done() {
	atomic.AddInt64(&wgc.counter, -1)
	wgc.wg.Done()
}

// Wait blocks until the WaitGroup counter is zero
func (wgc *WaitGroupCounter) Wait() {
	wgc.wg.Wait()
}

// Counter returns the current value of the counter
func (wgc *WaitGroupCounter) Counter() int {
	return int(atomic.LoadInt64(&wgc.counter))
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
	db           *sql.DB
	config       Config
	wg           sync.WaitGroup
	stop         chan struct{}
	taskBuffer   chan *Task
	bufferMutex  sync.Mutex
	activeWorker WaitGroupCounter
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
		db:         db,
		config:     config,
		stop:       make(chan struct{}),
		taskBuffer: make(chan *Task, config.TaskBufferSize),
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
	log.Println("Starting task processor with buffer size", tp.config.TaskBufferSize,
		"and max concurrent", tp.config.MaxConcurrent)

	// Start the task fetcher
	tp.wg.Add(1)
	go tp.taskFetcher()

	// Start the task workers
	for i := 0; i < tp.config.WorkerCount; i++ {
		tp.wg.Add(1)
		go tp.worker(i)
	}
}

// taskFetcher fetches tasks from the database and adds them to the buffer
func (tp *TaskProcessor) taskFetcher() {
	defer tp.wg.Done()

	log.Println("Task fetcher started")

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
			log.Println("Task fetcher stopping")
			close(tp.taskBuffer) // Signal workers that no more tasks will be coming
			return
		case <-healthCheckTicker.C:
			// Perform a health check on the database connection
			if err := tp.db.Ping(); err != nil {
				log.Printf("Database health check failed: %v", err)
				// We don't need to do anything else here, just log the issue
			} else {
				log.Println("Database health check passed")
			}
		case <-ticker.C:
			// Only fetch more tasks if buffer is below threshold (75% of capacity)
			bufferThreshold := tp.config.TaskBufferSize * 3 / 4
			if len(tp.taskBuffer) < bufferThreshold {
				batchSize := tp.config.TaskBufferSize - len(tp.taskBuffer)
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

				// Add tasks to buffer
				for _, task := range tasks {
					select {
					case tp.taskBuffer <- task:
						// Task added to buffer
					case <-tp.stop:
						return
					}
				}

				if len(tasks) > 0 {
					log.Printf("Fetched %d tasks, buffer now has %d tasks",
						len(tasks), len(tp.taskBuffer))
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
	tp.wg.Wait()
	tp.db.Close()
	log.Println("Task processor stopped")
}

// worker is the main worker loop
func (tp *TaskProcessor) worker(id int) {
	defer tp.wg.Done()

	log.Printf("Worker %d started", id)

	for {
		select {
		case <-tp.stop:
			log.Printf("Worker %d stopping", id)
			return
		case task, ok := <-tp.taskBuffer:
			if !ok {
				// Channel closed, exit
				log.Printf("Worker %d exiting: task buffer closed", id)
				return
			}

			// Process the task with concurrency control
			tp.activeWorker.Add(1)
			go func(t *Task) {
				defer tp.activeWorker.Done()

				// Process the task
				err := tp.executeTask(t)

				// Update task status
				if err := tp.updateTaskStatus(t, err); err != nil {
					log.Printf("Worker %d error updating task status: %v", id, err)
				}
			}(task)

			// If we've reached max concurrent tasks, wait for one to finish
			if tp.config.MaxConcurrent > 0 {
				for {
					tp.bufferMutex.Lock()
					activeCount := tp.activeWorker.Counter()
					tp.bufferMutex.Unlock()

					if activeCount < tp.config.MaxConcurrent {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
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
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Add task ID as header
	req.Header.Set("X-Task-ID", task.ID)

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

// insertTestTask inserts a test task for development purposes
func insertTestTask(db *sql.DB) error {
	id := uuid.New().String()
	payload := `{"action": "test", "data": {"key": "value"}}`

	_, err := db.Exec(
		`INSERT INTO task_pool (id, type, payload) VALUES (?, ?, ?)`,
		id, "test_task", payload,
	)
	if err != nil {
		return fmt.Errorf("failed to insert test task: %w", err)
	}

	log.Printf("Inserted test task with ID: %s", id)
	return nil
}

func main() {
	// Configuration
	config := Config{
		// Enhanced connection parameters to prevent "busy buffer" errors
		DBConnectionString: "taskuser:taskpassword@tcp(localhost:3306)/taskdb?parseTime=true&timeout=10s&readTimeout=10s&writeTimeout=10s&clientFoundRows=true&maxAllowedPacket=4194304&interpolateParams=true",
		WorkerCount:        2,
		LockDuration:       2 * time.Minute,
		PollInterval:       2 * time.Second,
		APIEndpoint:        "http://localhost:3000",
		TaskBufferSize:     50, // Reduced from 100
		MaxConcurrent:      20, // Reduced from 50
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

	if err := insertTestTask(processor.db); err != nil {
		log.Printf("Warning: %v", err)
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
