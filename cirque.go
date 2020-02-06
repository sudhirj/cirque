package cirque

import (
	"sync"
)

type indexedValue struct {
	value interface{}
	index int64
}

// NewCirque creates a FIFO parallel queue that runs a given processor function on each job, similar to a parallel Map.
//
// The method accepts a parallelism number, which the maximum number of jobs that are processed simultaneously,
// and a processor function that takes a job as input and returns a indexedValue as output. The processor function must be safe
// to call from multiple goroutines.
//
// It returns two channels, one into which inputs can be passed, and one from which outputs can be read.
// Closing the input channel will close the output channel after processing is complete. Do not close the output channel yourself.
func NewCirque(parallelism int64, processor func(interface{}) interface{}) (chan<- interface{}, <-chan interface{}) {
	input := make(chan interface{})
	output := make(chan interface{})

	processedJobs := make(chan indexedValue)
	processedSignal := make(chan struct{}, parallelism)
	go func() { // process inputs
		poolWaiter := sync.WaitGroup{}
		pool := make(chan indexedValue)

		// Start worker pool of specified size
		for workerID := int64(0); workerID < parallelism; workerID++ {
			poolWaiter.Add(1)
			go func() {
				for job := range pool {
					processedJobs <- indexedValue{
						value: processor(job.value),
						index: job.index,
					}
				}
				poolWaiter.Done()
			}()
		}

		index := int64(0)
		inProgressCount := int64(0)
		for job := range input {
			pool <- indexedValue{
				value: job,
				index: index,
			}
			index = index + 1
			inProgressCount = inProgressCount + 1

			for inProgressCount > parallelism {
				<-processedSignal
				inProgressCount = inProgressCount - 1
			}

		}
		close(pool)

		poolWaiter.Wait()
		close(processedJobs)
	}()

	go func() { // send outputs in order
		nextIndex := int64(0)
		storedResults := map[int64]indexedValue{}
		for res := range processedJobs {
			storedResults[res.index] = res
			canSend := true
			for canSend {
				if storedResult, ok := storedResults[nextIndex]; ok {
					output <- storedResult.value
					delete(storedResults, storedResult.index)
					nextIndex = nextIndex + 1
					processedSignal <- struct{}{}
				} else {
					canSend = false
				}
			}
		}
		close(output)
		close(processedSignal)
	}()

	return input, output
}
