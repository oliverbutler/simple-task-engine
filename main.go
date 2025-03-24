package main

import (
	"log"
	"os"
	"os/signal"
	"simple-task-engine/lib"
	"simple-task-engine/lib/types"
	"strconv"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

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
	config := types.Config{
		DBConnectionString:    dbConnectionString,
		LockDuration:          1 * time.Minute,
		PollInterval:          1 * time.Second,
		APIEndpoint:           apiEndpoint,
		MaxConcurrent:         maxConcurrent,
		BackoffStrategy:       lib.DefaultBackoffStrategy,
		MaxQueryBatchSize:     maxQueryBatchSize,
		TaskBufferSize:        taskBufferSize,
		BufferRefillThreshold: bufferRefillThreshold,
	}

	// Create and start the task processor
	processor, err := lib.NewTaskProcessor(config)
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
