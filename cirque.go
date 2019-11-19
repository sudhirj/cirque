package cirque

import (
	"sync"
	"sync/atomic"
	"time"
)

// NewCirque creates a FIFO parallel queue that runs a given processor function on each job, similar to a parallel Map.
//
// The method accepts a parallelism number, which the maximum number of jobs that are processed simultaneously,
// and a processor function that takes a job as input and returns a result as output. The processor function must be safe
// to call from multiple goroutines.
//
// It returns two channels, one into which inputs can be passed, and one from which outputs can be read.
// Closing the input channel will close the output channel after processing is complete. Do not close the output channel yourself.
func NewCirque(parallelism int64, processor func(interface{}) interface{}) (chan<- interface{}, <-chan interface{}) {
	input := make(chan interface{})
	output := make(chan interface{})

	stagedResults := sync.Map{}
	popSignal := sync.NewCond(&sync.Mutex{})
	pushSignal := sync.NewCond(&sync.Mutex{})

	var completeFlag int64 = 0 // ideally a boolean, but we want an atomic variable without typecasts
	var leadingIndex int64 = 0
	var followingIndex int64 = 0
	processingFinished := make(chan struct{})

	go func() { // process all the inputs
		popSignal.L.Lock()
		defer popSignal.L.Unlock()

		for job := range input {
			for (aR(&leadingIndex) - aR(&followingIndex)) >= parallelism {
				popSignal.Wait() // parallelism limit reached, wait until a pop happens
			}
			go func(i int64, j interface{}) {
				stagedResults.Store(i, processor(j))
				pushSignal.Broadcast() // store the result and broadcast a push event
			}(leadingIndex, job)
			aInc(&leadingIndex)
		}
		pushSignal.Broadcast() // when input channel is empty, broadcast push event to close output channel
		aInc(&completeFlag)
		// This is to avoid race condition where output channel does not close.
		for {
			select {
			case <-processingFinished:
				return

			case <-time.After(10 * time.Millisecond):
				pushSignal.Broadcast()
			}
		}
	}()

	go func() { // send outputs in order
		pushSignal.L.Lock()
		defer pushSignal.L.Unlock()

		for { // infinite loop that relies on the push event wait for holding
			if aR(&completeFlag) > 0 && aR(&followingIndex) == aR(&leadingIndex) {
				// all inputs have been accepted and we've caught up on outputs
				close(output)
				processingFinished <- struct{}{}
				return
			}

			if value, ok := stagedResults.Load(followingIndex); ok {
				output <- value
				go stagedResults.Delete(followingIndex) // clear the map behind us, keeps memory usage constant
				go popSignal.Broadcast()
				aInc(&followingIndex)
			} else {
				pushSignal.Wait()
			}
		}
	}()

	return input, output
}

// aInc atomically increments the given int
func aInc(i *int64) int64 {
	return atomic.AddInt64(i, 1)
}

// aR atomically reads the given int
func aR(i *int64) int64 {
	return atomic.LoadInt64(i)
}
