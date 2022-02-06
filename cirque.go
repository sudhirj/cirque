package cirque

import (
	"sync"
)

// NewCirque creates a FIFO parallel queue that runs a given processor function on each job, similar to a parallel Map.
//
// The method accepts a parallelism number, which the maximum number of jobs that are processed simultaneously,
// and a processor function that takes a job as input and returns a indexedValue as output. The processor function must be safe
// to call from multiple goroutines.
//
// It returns two channels, one into which inputs can be passed, and one from which outputs can be read.
// Closing the input channel will close the output channel after processing is complete. Do not close the output channel yourself.
func NewCirque[I any, O any](parallelism int64, processor func(I) O) (chan<- I, <-chan O) {
	input := make(chan I)
	output := make(chan O)

	inputHolder := make(map[int64]I)
	outputHolder := make(map[int64]O)

	inputLock := sync.RWMutex{}
	outputLock := sync.RWMutex{}

	processedJobs := make(chan int64)
	semaphore := make(chan struct{}, parallelism)
	go func() { // process inputs
		poolWaiter := sync.WaitGroup{}
		pool := make(chan int64)

		// Start worker pool of specified size
		for workerID := int64(0); workerID < parallelism; workerID++ {
			poolWaiter.Add(1)
			go func() {
				for index := range pool {
					inputLock.RLock()
					input := inputHolder[index]
					inputLock.RUnlock()

					output := processor(input)

					outputLock.Lock()
					outputHolder[index] = output
					outputLock.Unlock()

					inputLock.Lock()
					delete(inputHolder, index)
					inputLock.Unlock()

					processedJobs <- index
				}
				poolWaiter.Done()
			}()
		}

		index := int64(0)
		for job := range input {
			inputLock.Lock()
			inputHolder[index] = job
			inputLock.Unlock()

			pool <- index
			index = index + 1
			semaphore <- struct{}{}
		}
		close(pool)

		poolWaiter.Wait()
		close(processedJobs)
	}()

	go func() { // send outputs in order
		nextIndex := int64(0)
		for range processedJobs {
			canSend := true
			for canSend {
				outputLock.RLock()
				storedResult, ok := outputHolder[nextIndex]
				outputLock.RUnlock()

				if ok {
					output <- storedResult

					outputLock.Lock()
					delete(outputHolder, nextIndex)
					outputLock.Unlock()

					nextIndex = nextIndex + 1
					<-semaphore
				} else {
					canSend = false
				}
			}
		}
		close(output)
	}()

	return input, output
}
