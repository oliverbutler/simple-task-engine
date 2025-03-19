package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"simple-task-engine/store"
	"simple-task-engine/types"
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
	Id            string      `json:"id"`
	Type          string      `json:"type"`
	Priority      string      `json:"priority"`
	Payload       TaskPayload `json:"payload"`
	RetryCount    int         `json:"retry_count"`
	MaxRetryCount int         `json:"max_retry_count"`
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
		log.Println("Headers:")
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
		log.Println("Request Body:")
		log.Println(string(bodyBytes))

		apiTaskPayload := ApiTaskPayload{}
		err = json.NewDecoder(r.Body).Decode(&apiTaskPayload)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"status":"could not parse request body"}`))
			return
		}

		if apiTaskPayload.Type == "SendEmailFailOnce" && apiTaskPayload.RetryCount == 0 {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"status":"fail once"}`))
			return
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

	config := types.Config{
		DBConnectionString:    connectionString,
		DBTableName:           tableName,
		LockDuration:          10 * time.Second,
		PollInterval:          200 * time.Millisecond,
		APIEndpoint:           apiServer.URL,
		MaxConcurrent:         5,
		MaxQueryBatchSize:     10,
		TaskBufferSize:        20,
		BufferRefillThreshold: 0.5,
		BackoffStrategy: types.BackoffStrategy{
			Delays: []int{0}, // Fast retries for testing
		},
	}

	processor, err := NewTaskProcessor(config)
	if err != nil {
		t.Fatalf("Failed to create task processor: %v", err)
	}

	var id string
	id, err = CreateTask(processor.db, tableName, CreateTaskOptions{
		Type:     "SendEmail",
		Priority: PriorityHigh,
		Payload: TaskPayload{
			"email": "foo@example.com",
		},
	})

	var id2 string
	id2, err = CreateTask(processor.db, tableName, CreateTaskOptions{
		Type:     "SendEmailFailOnce",
		Priority: PriorityHigh,
		Payload: TaskPayload{
			"email": "foo@example.com",
		},
	})
	require.NoError(t, err)

	processor.Start()

	time.Sleep(3000 * time.Millisecond)

	processor.Stop()

	require.Equal(t, 2, len(apiStats.GetTasksProcessed()))
	require.Equal(t, ApiTaskPayload{
		Id:            id,
		Type:          "SendEmail",
		Priority:      "high",
		RetryCount:    0,
		MaxRetryCount: 5,
		Payload: TaskPayload{
			"email": "foo@example.com",
		},
	}, apiStats.GetTasksProcessed()[0])

	require.Equal(t, ApiTaskPayload{
		Id:            id2,
		Type:          "SendEmailFailOnce",
		Priority:      "high",
		RetryCount:    1,
		MaxRetryCount: 5,
		Payload: TaskPayload{
			"email": "foo@example.com",
		},
	}, apiStats.GetTasksProcessed()[1])
}

func TestGetTasksForProcessing(t *testing.T) {
	tableName, db := CreateNewTaskPoolTable(t)
	defer func() {
		db.Close()
	}()

	repo := store.NewTaskRepositoryMySQL(db, tableName, 1*time.Minute)

	id, err := CreateTask(db, tableName, CreateTaskOptions{
		Type:     "SendEmail",
		Priority: PriorityHigh,
		Payload: TaskPayload{
			"email": "foo@example.com",
		},
	})
	require.NoError(t, err)

	tasks, err := repo.GetTasksForProcessing(100)

	require.Equal(t, 1, len(tasks))
	require.Equal(t, id, tasks[0].ID)
}
