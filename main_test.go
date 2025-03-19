package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
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

type ApiTaskPayload struct {
	Type string `json:"type"`
}

type ApiStats struct {
	tasksProcessed []ApiTaskPayload
}

func (a *ApiStats) RecordTaskProcessed(task ApiTaskPayload) {
	a.tasksProcessed = append(a.tasksProcessed, task)
}

func (a *ApiStats) GetTasksProcessed() []ApiTaskPayload {
	return a.tasksProcessed
}

func CreateFakeAPI() (*httptest.Server, *ApiStats) {
	apiStats := &ApiStats{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Debug: Log the request method and URL
		fmt.Printf("Received %s request to %s\n", r.Method, r.URL.String())

		// Debug: Log request headers
		fmt.Println("Headers:")
		for key, values := range r.Header {
			for _, value := range values {
				fmt.Printf("  %s: %s\n", key, value)
			}
		}

		// Debug: Read and log the request body
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			fmt.Printf("Error reading body: %v\n", err)
		}

		// Close the original body
		r.Body.Close()

		// Restore the body for further processing
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

		// Debug: Log the body content
		fmt.Println("Request Body:")
		fmt.Println(string(bodyBytes))
		path := r.URL.Path
		var taskType string

		// Check if the path follows the pattern /task/:name
		pathParts := strings.Split(path, "/")
		if len(pathParts) >= 3 && pathParts[1] == "task" {
			// Extract the name parameter (the 3rd part of the path)
			taskType = pathParts[2]
		}

		apiTaskPayload := ApiTaskPayload{
			Type: taskType,
		}

		apiStats.RecordTaskProcessed(apiTaskPayload)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"success"}`))
	}))

	return server, apiStats
}

func TestMain(t *testing.T) {
	tableName, db := CreateNewTaskPoolTable(t)
	defer func() {
		db.Close()
	}()

	apiServer, apiStats := CreateFakeAPI()
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

	require.Equal(t, 1, len(apiStats.GetTasksProcessed()))
	require.Equal(t, "SendEmail", apiStats.GetTasksProcessed()[0].Type)

	require.Equal(t, "foo", "foo")
}
