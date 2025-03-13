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
	"unsafe"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

// Counter returns the current value of the WaitGroup counter
// This is a hack to access the counter field of sync.WaitGroup
func (wg *sync.WaitGroup) Counter() uint64 {
	// WaitGroup has three uint32 fields: counter, waiters, and sema
	// We want to access the first field (counter)
	// This is unsafe and may break with Go updates, but useful for monitoring
	return atomic.LoadUint64((*uint64)(unsafe.Pointer(wg)))
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
	activeWorker sync.WaitGroup
}

// NewTaskProcessor creates a new task processor
func NewTaskProcessor(config Config) (*TaskProcessor, error) {
	db, err := sql.Open("mysql", config.DBConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Create the task_pool table if it doesn't exist
	if err := createTaskPoolTableIfNotExists(db); err != nil {
		return nil, fmt.Errorf("failed to create task_pool table: %w", err)
	}

	return &TaskProcessor{
		db:         db,
		config:     config,
		stop:       make(chan struct{}),
		taskBuffer: make(chan *Task, config.TaskBufferSize),
	}, nil
}

// createTaskPoolTableIfNotExists creates the task_pool table if it doesn't exist
func createTaskPoolTableIfNotExists(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS task_pool (
		id VARCHAR(36) PRIMARY KEY,
		idempotency_key VARCHAR(255) NULL,
		type VARCHAR(255) NOT NULL,
		priority VARCHAR(50) DEFAULT 'normal',
		payload JSON NOT NULL,
		status VARCHAR(50) DEFAULT 'pending',
		locked_until DATETIME NULL,
		retry_count INT DEFAULT 0,
		max_retry_count INT DEFAULT 3,
		last_error TEXT NULL,
		process_after DATETIME DEFAULT CURRENT_TIMESTAMP,
		correlation_id VARCHAR(255) NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		INDEX idx_status_process (status, process_after),
		INDEX idx_priority (priority)
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
	
	for {
		select {
		case <-tp.stop:
			log.Println("Task fetcher stopping")
			close(tp.taskBuffer) // Signal workers that no more tasks will be coming
			return
		case <-ticker.C:
			// Only fetch more tasks if buffer is below threshold (75% of capacity)
			bufferThreshold := tp.config.TaskBufferSize * 3 / 4
			if len(tp.taskBuffer) < bufferThreshold {
				batchSize := tp.config.TaskBufferSize - len(tp.taskBuffer)
				tasks, err := tp.fetchTasks(batchSize)
				if err != nil {
					log.Printf("Error fetching tasks: %v", err)
					continue
				}
				
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
	// Begin transaction
	tx, err := tp.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	
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
	
	rows, err := tx.Query(query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query tasks: %w", err)
	}
	defer rows.Close()
	
	var tasks []*Task
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
		
		// Lock the task
		_, err = tx.Exec(
			"UPDATE task_pool SET status = 'processing', locked_until = ? WHERE id = ?",
			lockedUntil, task.ID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to lock task: %w", err)
		}
		
		tasks = append(tasks, &task)
	}
	
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tasks: %w", err)
	}
	
	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
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
					var activeCount int
					func() {
						tp.bufferMutex.Lock()
						defer tp.bufferMutex.Unlock()
						activeCount = int(tp.activeWorker.Counter())
					}()
					
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
func (tp *TaskProcessor) updateTaskStatus(task *Task, err error) error {
	// Begin a new transaction to update the task status
	tx, err := tp.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin update transaction: %w", err)
	}
	defer tx.Rollback()

	if err == nil {
		// Task succeeded
		if err = tp.markTaskCompleted(tx, task); err != nil {
			return fmt.Errorf("failed to mark task as completed: %w", err)
		}
	} else {
		// Task failed
		if err = tp.markTaskFailed(tx, task, err.Error()); err != nil {
			return fmt.Errorf("failed to mark task as failed: %w", err)
		}
	}

	// Commit the status update
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit status update: %w", err)
	}

	return nil
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
		DBConnectionString: "taskuser:taskpassword@tcp(localhost:3306)/taskdb?parseTime=true",
		WorkerCount:        5,
		LockDuration:       5 * time.Minute,
		PollInterval:       5 * time.Second,
		APIEndpoint:        "http://localhost:3000",
		TaskBufferSize:     100,
		MaxConcurrent:      50,
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

	// Insert a test task if in development mode
	if os.Getenv("ENV") == "development" {
		if err := insertTestTask(processor.db); err != nil {
			log.Printf("Warning: %v", err)
		}
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
