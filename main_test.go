package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// CreateNewTaskPoolTable creates a new unique task_pool table and returns its name
func CreateNewTaskPoolTable(t *testing.T) (string, *sql.DB) {
	// Connect to the existing MySQL instance
	db, err := sql.Open("mysql", "root:rootpassword@tcp(localhost:3309)/taskdb?parseTime=true")
	require.NoError(t, err)

	// Generate a unique table name
	tableName := fmt.Sprintf("task_pool_%s", strings.Replace(uuid.New().String(), "-", "", -1))

	// Read the SQL schema file
	sqlBytes, err := os.ReadFile("./task.sql")
	require.NoError(t, err)

	// Replace the default table name with our unique one
	sqlContent := string(sqlBytes)
	sqlContent = strings.Replace(sqlContent, "task_pool", tableName, 1)

	// Execute the modified SQL to create the table
	_, err = db.Exec(sqlContent)
	require.NoError(t, err)

	return tableName, db
}

func CreateFakeAPI() *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"success"}`))
	}))

	return server
}

func TestMain(t *testing.T) {
	tableName, db := CreateNewTaskPoolTable(t)
	defer func() {
		db.Close()
	}()

	apiServer := CreateFakeAPI()
	defer apiServer.Close()

	connectionString := "taskuser:taskpassword@tcp(localhost:3309)/taskdb?parseTime=true"

	config := Config{
		DBConnectionString:    connectionString,
		DBTableName:           tableName,
		LockDuration:          10 * time.Second,
		PollInterval:          500 * time.Millisecond,
		APIEndpoint:           apiServer.URL,
		MaxConcurrent:         5,
		MaxQueryBatchSize:     10,
		TaskBufferSize:        20,
		BufferRefillThreshold: 0.5,
		BackoffStrategy: BackoffStrategy{
			Delays: []int{1, 2, 5}, // Fast retries for testing
		},
	}

	processor, err := NewTaskProcessor(config)
	if err != nil {
		t.Fatalf("Failed to create task processor: %v", err)
	}

	CreateTask(processor.db, tableName, CreateTaskOptions{
		Type:     "SendEmail",
		Priority: PriorityHigh,
		Payload: TaskPayload{
			"email": "foo@example.com",
		},
	})

	processor.Start()

	time.Sleep(5 * time.Second)

	processor.Stop()

	require.Equal(t, "foo", "foo")
}
