package lib

import (
	"simple-task-engine/lib/helpers"
	"simple-task-engine/lib/types"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBufferManagementLoop(t *testing.T) {
	tableName, db := CreateNewTaskPoolTable(t)
	defer func() {
		db.Close()
	}()

	connectionString := "taskuser:taskpassword@tcp(localhost:3309)/taskdb?parseTime=true"

	config := types.Config{
		DBConnectionString:    connectionString,
		DBTableName:           tableName,
		LockDuration:          10 * time.Second,
		PollInterval:          200 * time.Millisecond,
		APIEndpoint:           "/noop",
		MaxConcurrent:         5,
		MaxQueryBatchSize:     10,
		TaskBufferSize:        20,
		BufferRefillThreshold: 0.5,
		RegisterMetrics:       false,
		BackoffStrategy: types.BackoffStrategy{
			Delays: []int{0}, // Fast retries for testing
		},
	}

	processor, err := NewTaskProcessor(config)
	if err != nil {
		t.Fatalf("Failed to create task processor: %v", err)
	}

	processor.wg.Add(1)
	go processor.taskBufferLoop()

	id, err := helpers.CreateTask(db, tableName, helpers.CreateTaskOptions{
		Type:     "SendEmail",
		Priority: helpers.PriorityHigh,
		Payload: helpers.TaskPayload{
			"email": "foo@example.com",
		},
	})
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	processor.stop <- struct{}{}

	require.Equal(t, 1, len(processor.taskBuffer))
	require.Equal(t, id, processor.taskBuffer[0].ID)
}
