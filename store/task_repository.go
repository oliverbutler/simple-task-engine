package store

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"simple-task-engine/types"
	"strings"
	"time"
)

type TaskRepository interface {
	GetTasksForProcessing(taskFetchLimit int) ([]*types.Task, error)
}

type TaskRepositoryMySQL struct {
	db           *sql.DB
	tableName    string
	lockDuration time.Duration
}

func NewTaskRepositoryMySQL(db *sql.DB, tableName string, lockDuration time.Duration) TaskRepository {
	return &TaskRepositoryMySQL{db: db, tableName: tableName, lockDuration: lockDuration}
}

func (r *TaskRepositoryMySQL) GetTasksForProcessing(taskFetchLimit int) ([]*types.Task, error) {
	log.Printf("Fetching up to %d tasks from %s", taskFetchLimit, r.tableName)

	// Start a transaction with a context timeout to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, nil)
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
	`, r.tableName)

	// Use context with timeout for the query
	rows, err := tx.QueryContext(ctx, query, taskFetchLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to query tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*types.Task
	var taskIDs []string
	lockedUntil := time.Now().Add(r.lockDuration)

	for rows.Next() {
		var task types.Task
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
		args := make([]any, len(taskIDs)+1)
		args[0] = lockedUntil

		for i, id := range taskIDs {
			placeholders[i] = "?"
			args[i+1] = id
		}

		updateQuery := fmt.Sprintf(
			"UPDATE %s SET status = 'processing', locked_until = ? WHERE id IN (%s)",
			r.tableName,
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
