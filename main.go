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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

// Task represents a task from the database table
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
	DBTableName        string
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

// Metrics holds all Prometheus metrics for the application
type Metrics struct {
	// Task processing metrics
	TasksProcessed prometheus.Counter
	TasksSucceeded prometheus.Counter
	TasksFailed    prometheus.Counter
	TasksRetried   prometheus.Counter

	// Buffer metrics
	BufferSize     prometheus.Gauge
	BufferCapacity prometheus.Gauge

	// In-flight metrics
	InFlightTasks      prometheus.Gauge
	MaxConcurrentTasks prometheus.Gauge

	// Channel metrics
	ResultsChannelSize prometheus.Gauge
	ResultsChannelCap  prometheus.Gauge

	// Database metrics
	DBConnections        prometheus.Gauge
	DBErrors             prometheus.Counter
	DBQueryCount         prometheus.Counter
	DBQueryRowsSelected  prometheus.Counter
	DBUpdateCount        prometheus.Counter
	DBUpdateRowsAffected prometheus.Counter
	DBQueryDuration      prometheus.Histogram
	DBUpdateDuration     prometheus.Histogram

	// API metrics
	APIRequestDuration prometheus.Histogram
	APIRequestsTotal   prometheus.Counter
	APIRequestErrors   prometheus.Counter

	// Task processing time
	TaskProcessingDuration prometheus.Histogram

	// Task types
	TasksByType *prometheus.CounterVec

	// Task priorities
	TasksByPriority *prometheus.CounterVec

	// Task status updates
	TaskStatusUpdates *prometheus.CounterVec
}

// NewMetrics creates and registers all Prometheus metrics
func NewMetrics() *Metrics {
	m := &Metrics{
		TasksProcessed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_tasks_processed_total",
			Help: "The total number of processed tasks",
		}),
		TasksSucceeded: promauto.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_tasks_succeeded_total",
			Help: "The total number of successfully processed tasks",
		}),

		TasksFailed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_tasks_failed_total",
			Help: "The total number of failed tasks",
		}),
		TasksRetried: promauto.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_tasks_retried_total",
			Help: "The total number of retried tasks",
		}),
		BufferSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_buffer_size",
			Help: "Current number of tasks in the buffer",
		}),
		BufferCapacity: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_buffer_capacity",
			Help: "Maximum capacity of the task buffer",
		}),
		InFlightTasks: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_inflight_tasks",
			Help: "Current number of in-flight tasks",
		}),
		MaxConcurrentTasks: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_max_concurrent_tasks",
			Help: "Maximum number of concurrent tasks",
		}),
		ResultsChannelSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_results_channel_size",
			Help: "Current size of the results channel",
		}),
		ResultsChannelCap: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_results_channel_capacity",
			Help: "Capacity of the results channel",
		}),
		DBConnections: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_db_connections",
			Help: "Current number of database connections",
		}),
		DBErrors: promauto.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_db_errors_total",
			Help: "Total number of database errors",
		}),
		DBQueryCount: promauto.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_db_query_count_total",
			Help: "Total number of database query operations",
		}),
		DBQueryRowsSelected: promauto.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_db_query_rows_selected_total",
			Help: "Total number of rows selected from database",
		}),
		DBUpdateCount: promauto.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_db_update_count_total",
			Help: "Total number of database update operations",
		}),
		DBUpdateRowsAffected: promauto.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_db_update_rows_affected_total",
			Help: "Total number of rows affected by database updates",
		}),
		DBQueryDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "task_engine_db_query_duration_seconds",
			Help:    "Duration of database query operations in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		DBUpdateDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "task_engine_db_update_duration_seconds",
			Help:    "Duration of database update operations in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		APIRequestDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "task_engine_api_request_duration_seconds",
			Help:    "Duration of API requests in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		APIRequestsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_api_requests_total",
			Help: "Total number of API requests",
		}),
		APIRequestErrors: promauto.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_api_request_errors_total",
			Help: "Total number of API request errors",
		}),
		TaskProcessingDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "task_engine_task_processing_duration_seconds",
			Help:    "Duration of task processing in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		TasksByType: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "task_engine_tasks_by_type_total",
				Help: "Total number of tasks by type",
			},
			[]string{"type"},
		),
		TasksByPriority: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "task_engine_tasks_by_priority_total",
				Help: "Total number of tasks by priority",
			},
			[]string{"priority"},
		),
		TaskStatusUpdates: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "task_engine_task_status_updates_total",
				Help: "Total number of task status updates by status",
			},
			[]string{"status"},
		),
	}

	return m
}

// TaskProcessor handles task processing
type TaskProcessor struct {
	db              *sql.DB
	config          Config
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
	taskBuffer      []*Task
	taskBufferMutex sync.RWMutex
	metrics         *Metrics
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
	metrics := NewMetrics()

	// Set initial values for capacity metrics
	metrics.BufferCapacity.Set(float64(config.TaskBufferSize))
	metrics.MaxConcurrentTasks.Set(float64(config.MaxConcurrent))
	metrics.ResultsChannelCap.Set(float64(resultsBufferSize))

	processor := &TaskProcessor{
		db:            db,
		config:        config,
		stop:          make(chan struct{}),
		inFlightTasks: make(map[string]*InFlightTask),
		resultsChan:   make(chan TaskResult, resultsBufferSize),
		batchTicker:   time.NewTicker(1 * time.Second), // Process results every second
		shutdownMode:  false,
		taskBuffer:    make([]*Task, 0, config.TaskBufferSize),
		metrics:       metrics,
	}

	// Start a connection monitor goroutine
	go processor.monitorDatabaseConnection()

	// Start metrics updater
	go processor.updateMetrics()

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
	go tp.processLoop()

	tp.wg.Add(1)
	go tp.bufferManagementLoop()

	tp.wg.Add(1)
	go tp.resultProcessor()

	go tp.startMetricsServer(":9090")
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

// fetchTasks fetches a batch of tasks from the database using FOR UPDATE SKIP LOCKED
func (tp *TaskProcessor) fetchTasks(taskFetchLimit int) ([]*Task, error) {
	// Check connection before starting transaction
	if err := tp.db.Ping(); err != nil {
		log.Printf("Database connection check failed: %v", err)
		tp.metrics.DBErrors.Inc()
		return nil, fmt.Errorf("database connection check failed: %w", err)
	}

	log.Printf("Fetching up to %d tasks", taskFetchLimit)

	// Start a transaction with a context timeout to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := tp.db.BeginTx(ctx, nil)
	if err != nil {
		tp.metrics.DBErrors.Inc()
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Select and lock eligible tasks in a single operation
	// FOR UPDATE SKIP LOCKED ensures we only get tasks that aren't locked by other processes
	query := fmt.Sprintf(`
		SELECT id, idempotency_key, type, priority, payload, status, 
		       locked_until, retry_count, max_retry_count, last_error, 
		       process_after, correlation_id, created_at, updated_at
		FROM %s
		WHERE status = 'pending'
		  AND process_after <= NOW()
		  AND (locked_until IS NULL OR locked_until <= NOW())
		ORDER BY priority DESC, process_after ASC
		LIMIT ?
		FOR UPDATE SKIP LOCKED
	`, tp.config.DBTableName)

	// Use context with timeout for the query
	rows, err := tx.QueryContext(ctx, query, taskFetchLimit)
	if err != nil {
		tp.metrics.DBErrors.Inc()
		return nil, fmt.Errorf("failed to query tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*Task
	var taskIDs []string
	lockedUntil := time.Now().Add(tp.config.LockDuration)

	// Start timing the query
	queryStartTime := time.Now()
	defer func() {
		tp.metrics.DBQueryDuration.Observe(time.Since(queryStartTime).Seconds())
		tp.metrics.DBQueryCount.Inc()
	}()

	for rows.Next() {
		var task Task
		err := rows.Scan(
			&task.ID, &task.IdempotencyKey, &task.Type, &task.Priority, &task.Payload,
			&task.Status, &task.LockedUntil, &task.RetryCount, &task.MaxRetryCount,
			&task.LastError, &task.ProcessAfter, &task.CorrelationID, &task.CreatedAt,
			&task.UpdatedAt,
		)
		if err != nil {
			tp.metrics.DBErrors.Inc()
			return nil, fmt.Errorf("failed to scan task: %w", err)
		}

		tasks = append(tasks, &task)
		taskIDs = append(taskIDs, task.ID)
		tp.metrics.DBQueryRowsSelected.Inc()
	}

	if err = rows.Err(); err != nil {
		tp.metrics.DBErrors.Inc()
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
		args := make([]any, len(taskIDs)+1)
		args[0] = lockedUntil

		for i, id := range taskIDs {
			placeholders[i] = "?"
			args[i+1] = id
		}

		updateQuery := fmt.Sprintf(
			"UPDATE %s SET status = 'processing', locked_until = ? WHERE id IN (%s)",
			tp.config.DBTableName,
			strings.Join(placeholders, ","),
		)

		// Start timing the update
		updateStartTime := time.Now()

		// Use context with timeout for the update
		result, err := tx.ExecContext(ctx, updateQuery, args...)

		// Record update duration
		tp.metrics.DBUpdateDuration.Observe(time.Since(updateStartTime).Seconds())
		tp.metrics.DBUpdateCount.Inc()

		if err != nil {
			tp.metrics.DBErrors.Inc()
			return nil, fmt.Errorf("failed to update tasks to processing status: %w", err)
		}

		// Count affected rows if possible
		if rowsAffected, err := result.RowsAffected(); err == nil {
			tp.metrics.DBUpdateRowsAffected.Add(float64(rowsAffected))
		}
	}

	// Commit the transaction with a timeout
	_, commitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer commitCancel()

	if err := tx.Commit(); err != nil {
		tp.metrics.DBErrors.Inc()
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

	// Enter shutdown mode before stopping
	tp.setShutdownMode(true)
	log.Println("Entered shutdown mode - no new tasks will be fetched")

	// Signal all components to stop adding new tasks
	close(tp.stop)
	tp.batchTicker.Stop()

	// Give in-flight tasks some time to complete
	log.Println("Waiting for in-flight tasks to complete (max 30 seconds)...")

	// Create a ticker to check and report progress
	progressTicker := time.NewTicker(5 * time.Second)
	defer progressTicker.Stop()

	// Create a more frequent ticker for debugging in-flight tasks
	debugTicker := time.NewTicker(1 * time.Second)
	defer debugTicker.Stop()

	// Create a ticker to actively process results during shutdown
	processingTicker := time.NewTicker(500 * time.Millisecond)
	defer processingTicker.Stop()

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

	// Shutdown metrics server gracefully
	if tp.metricsServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := tp.metricsServer.Shutdown(ctx); err != nil {
			log.Printf("Error shutting down metrics server: %v", err)
		}
	}

	// Close the database connection
	tp.db.Close()
	log.Println("Task processor stopped")
}

// executeTask processes a task by calling the API
func (tp *TaskProcessor) executeTask(task *Task) error {
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
		result, err := tx.ExecContext(ctx, updateQuery, args...)

		// Record update duration
		updateStartTime := time.Now()
		defer func() {
			tp.metrics.DBUpdateDuration.Observe(time.Since(updateStartTime).Seconds())
			tp.metrics.DBUpdateCount.Inc()
		}()

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

			// Count affected rows if possible
			if rowsAffected, err := result.RowsAffected(); err == nil {
				tp.metrics.DBUpdateRowsAffected.Add(float64(rowsAffected))
			}
		}
	}

	// Update failed tasks in batch if any
	for id, errMsg := range failedTasks {
		updateStartTime := time.Now()
		defer func() {
			tp.metrics.DBUpdateDuration.Observe(time.Since(updateStartTime).Seconds())
			tp.metrics.DBUpdateCount.Inc()
		}()

		result, err := tx.ExecContext(ctx,
			fmt.Sprintf("UPDATE %s SET status = 'failed', retry_count = retry_count + 1, locked_until = NULL, last_error = ? WHERE id = ?", tp.config.DBTableName),
			errMsg, id,
		)

		if err != nil {
			log.Printf("Failed to update failed task %s: %v", id, err)
		} else {
			log.Printf("Marked task %s as permanently failed", id)
			tp.metrics.TaskStatusUpdates.WithLabelValues("failed").Inc()

			// Count affected rows if possible
			if rowsAffected, err := result.RowsAffected(); err == nil {
				tp.metrics.DBUpdateRowsAffected.Add(float64(rowsAffected))
			}
		}
	}

	// Update retry tasks in batch if any
	for id, info := range retryTasks {
		updateStartTime := time.Now()
		defer func() {
			tp.metrics.DBUpdateDuration.Observe(time.Since(updateStartTime).Seconds())
			tp.metrics.DBUpdateCount.Inc()
		}()

		result, err := tx.ExecContext(ctx,
			fmt.Sprintf("UPDATE %s SET status = 'pending', retry_count = ?, process_after = ?, locked_until = NULL, last_error = ? WHERE id = ?", tp.config.DBTableName),
			info.retryCount, info.processAfter, info.errorMsg, id,
		)

		if err != nil {
			log.Printf("Failed to update retry task %s: %v", id, err)
		} else {
			log.Printf("Scheduled task %s for retry attempt %d", id, info.retryCount)
			tp.metrics.TaskStatusUpdates.WithLabelValues("pending").Inc()

			// Count affected rows if possible
			if rowsAffected, err := result.RowsAffected(); err == nil {
				tp.metrics.DBUpdateRowsAffected.Add(float64(rowsAffected))
			}
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
		tp.metrics.DBErrors.Inc()
		return fmt.Errorf("database connection check failed: %w", err)
	}

	var updateErr error
	for retries := 0; retries < 5; retries++ {
		updateStartTime := time.Now()
		defer func() {
			tp.metrics.DBUpdateDuration.Observe(time.Since(updateStartTime).Seconds())
			tp.metrics.DBUpdateCount.Inc()
		}()

		if taskErr == nil {
			// Task succeeded - direct update without transaction
			result, updateErr := tp.db.ExecContext(ctx,
				fmt.Sprintf("UPDATE %s SET status = 'completed', locked_until = NULL WHERE id = ?", tp.config.DBTableName),
				task.ID,
			)

			if updateErr == nil {
				log.Printf("Marked task %s as completed", task.ID)
				tp.metrics.TasksSucceeded.Inc()
				tp.metrics.TaskStatusUpdates.WithLabelValues("completed").Inc()

				// Count affected rows if possible
				if rowsAffected, err := result.RowsAffected(); err == nil {
					tp.metrics.DBUpdateRowsAffected.Add(float64(rowsAffected))
				}
				return nil
			}
		} else {
			// Task failed - direct update without transaction
			task.RetryCount++

			if task.RetryCount >= task.MaxRetryCount {
				// Max retries reached, mark as failed
				result, updateErr := tp.db.ExecContext(ctx,
					fmt.Sprintf("UPDATE %s SET status = 'failed', retry_count = ?, locked_until = NULL, last_error = ? WHERE id = ?", tp.config.DBTableName),
					task.RetryCount, taskErr.Error(), task.ID,
				)

				if updateErr == nil {
					log.Printf("Task %s failed permanently after %d retries: %s", task.ID, task.RetryCount, taskErr.Error())
					tp.metrics.TasksFailed.Inc()
					tp.metrics.TaskStatusUpdates.WithLabelValues("failed").Inc()

					// Count affected rows if possible
					if rowsAffected, err := result.RowsAffected(); err == nil {
						tp.metrics.DBUpdateRowsAffected.Add(float64(rowsAffected))
					}
					return nil
				}
			} else {
				// Calculate backoff using the strategy
				backoff := tp.config.BackoffStrategy.calculateBackoff(task.RetryCount)
				processAfter := time.Now().Add(backoff)

				result, updateErr := tp.db.ExecContext(ctx,
					fmt.Sprintf("UPDATE %s SET status = 'pending', retry_count = ?, process_after = ?, locked_until = NULL, last_error = ? WHERE id = ?", tp.config.DBTableName),
					task.RetryCount, processAfter, taskErr.Error(), task.ID,
				)

				if updateErr == nil {
					log.Printf("Task %s failed, scheduled retry %d/%d after %s: %s",
						task.ID, task.RetryCount, task.MaxRetryCount, backoff, taskErr.Error())
					tp.metrics.TasksRetried.Inc()
					tp.metrics.TaskStatusUpdates.WithLabelValues("pending").Inc()

					// Count affected rows if possible
					if rowsAffected, err := result.RowsAffected(); err == nil {
						tp.metrics.DBUpdateRowsAffected.Add(float64(rowsAffected))
					}
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
func (tp *TaskProcessor) getTasksFromBuffer(count int) []*Task {
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
	defer func() {
		tp.wg.Done()
		log.Println("bufferManagementLoop > stopped")
	}()
	log.Println("bufferManagementLoop > started")

	pollTicker := time.NewTicker(tp.config.PollInterval)
	defer pollTicker.Stop()

	consecutiveFailures := 0

	for {
		select {
		case <-tp.stop:
			log.Println("Buffer management loop stopping")
			return

		case <-pollTicker.C:
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
	if !tp.tryFillBuffer() {
		return 0, nil
	}
	defer tp.unsetFillBuffer()

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
	fetchCount := min(spaceAvailable, tp.config.MaxQueryBatchSize)

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

			// Update DB stats if available
			stats := tp.db.Stats()
			tp.metrics.DBConnections.Set(float64(stats.OpenConnections))
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

func main() {
	// Read configuration from environment variables
	dbConnectionString := os.Getenv("DB_CONNECTION_STRING")
	if dbConnectionString == "" {
		dbConnectionString = "taskuser:taskpassword@tcp(localhost:3306)/taskdb?parseTime=true&timeout=5s&readTimeout=5s&writeTimeout=5s&clientFoundRows=true&maxAllowedPacket=4194304&interpolateParams=true"
	}

	apiEndpoint := os.Getenv("API_ENDPOINT")
	if apiEndpoint == "" {
		apiEndpoint = "http://localhost:3000"
	}

	maxConcurrent := 50
	if maxConcurrentStr := os.Getenv("MAX_CONCURRENT"); maxConcurrentStr != "" {
		if val, err := strconv.Atoi(maxConcurrentStr); err == nil && val > 0 {
			maxConcurrent = val
		}
	}

	taskBufferSize := 600
	if taskBufferSizeStr := os.Getenv("TASK_BUFFER_SIZE"); taskBufferSizeStr != "" {
		if val, err := strconv.Atoi(taskBufferSizeStr); err == nil && val > 0 {
			taskBufferSize = val
		}
	}

	maxQueryBatchSize := 200
	if maxQueryBatchSizeStr := os.Getenv("MAX_QUERY_BATCH_SIZE"); maxQueryBatchSizeStr != "" {
		if val, err := strconv.Atoi(maxQueryBatchSizeStr); err == nil && val > 0 {
			maxQueryBatchSize = val
		}
	}

	bufferRefillThreshold := 0.5
	if bufferRefillThresholdStr := os.Getenv("BUFFER_REFILL_THRESHOLD"); bufferRefillThresholdStr != "" {
		if val, err := strconv.ParseFloat(bufferRefillThresholdStr, 64); err == nil && val > 0 && val < 1 {
			bufferRefillThreshold = val
		}
	}

	// Configuration
	config := Config{
		DBConnectionString:    dbConnectionString,
		LockDuration:          1 * time.Minute,
		PollInterval:          1 * time.Second,
		APIEndpoint:           apiEndpoint,
		MaxConcurrent:         maxConcurrent,
		BackoffStrategy:       DefaultBackoffStrategy,
		MaxQueryBatchSize:     maxQueryBatchSize,
		TaskBufferSize:        taskBufferSize,
		BufferRefillThreshold: bufferRefillThreshold,
	}

	// Create and start the task processor
	processor, err := NewTaskProcessor(config)
	if err != nil {
		log.Fatalf("Failed to create task processor: %v", err)
	}

	// Start the processor
	processor.Start()

	log.Println("Task processor started")

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
