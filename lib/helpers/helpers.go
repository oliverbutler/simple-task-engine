package helpers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// TaskPriority represents the priority level of a task
type TaskPriority string

const (
	PriorityLow      TaskPriority = "low"
	PriorityMedium   TaskPriority = "medium"
	PriorityHigh     TaskPriority = "high"
	PriorityCritical TaskPriority = "critical"
)

// TaskStatus represents the status of a task
type TaskStatus string

const (
	StatusPending    TaskStatus = "pending"
	StatusProcessing TaskStatus = "processing"
	StatusCompleted  TaskStatus = "completed"
	StatusFailed     TaskStatus = "failed"
	StatusCancelled  TaskStatus = "cancelled"
	StatusScheduled  TaskStatus = "scheduled"
)

// TaskPayload represents the payload of a task
type TaskPayload map[string]any

// CreateTaskOptions contains options for creating a task
type CreateTaskOptions struct {
	Type           string
	Priority       TaskPriority
	Payload        TaskPayload
	Status         TaskStatus
	ProcessAfter   *time.Time
	MaxRetryCount  *int
	IdempotencyKey string
	CorrelationID  string
}

// CreateTask creates a new task in the database
func CreateTask(db *sql.DB, table_name string, options CreateTaskOptions) (string, error) {
	// Generate a new UUID for the task ID
	taskID := uuid.New().String()

	// Set default values if not provided
	if options.Priority == "" {
		options.Priority = PriorityMedium
	}

	if options.Status == "" {
		options.Status = StatusPending
	}

	if options.ProcessAfter == nil {
		now := time.Now()
		options.ProcessAfter = &now
	}

	if options.MaxRetryCount == nil {
		defaultRetryCount := 5
		options.MaxRetryCount = &defaultRetryCount
	}

	// Generate idempotency key if not provided
	if options.IdempotencyKey == "" {
		options.IdempotencyKey = fmt.Sprintf("task-%s", uuid.New().String())
	}

	// Convert payload to JSON
	payloadJSON, err := json.Marshal(options.Payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (
			id, 
			idempotency_key, 
			type, 
			priority, 
			payload, 
			status, 
			process_after, 
			max_retry_count,
			correlation_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, table_name)

	_, err = db.Exec(
		query,
		taskID,
		options.IdempotencyKey,
		options.Type,
		options.Priority,
		payloadJSON,
		options.Status,
		options.ProcessAfter,
		options.MaxRetryCount,
		options.CorrelationID,
	)
	if err != nil {
		return "", fmt.Errorf("failed to insert task: %w", err)
	}

	return taskID, nil
}
