package types

import (
	"database/sql"
	"encoding/json"
	"time"
)

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

// CalculateBackoff determines how long to wait before the next retry
func (bs BackoffStrategy) CalculateBackoff(retryCount int) time.Duration {
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
