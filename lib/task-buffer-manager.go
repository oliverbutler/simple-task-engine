package lib

import (
	"log"
	"math"
	"time"
)

func (tp *TaskProcessor) taskBufferLoop() {
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
	tasks, err := tp.taskRepository.GetTasksForProcessing(fetchCount)
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
