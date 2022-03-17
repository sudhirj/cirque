package cirque

import (
	"sync"
)

// NewCirque creates a FIFO parallel queue that runs a given
// processor function on each job, similar to a parallel Map.
//
// The method accepts a parallelism number, which the maximum
// number of jobs that are processed simultaneously,
// and a processor function that takes an input and returns
// an output. The processor function must be safe
// to call from multiple goroutines.
//
// It returns two channels, one into which inputs can be passed,
// and one from which outputs can be read. Closing the input channel
// will close the output channel after processing is complete. Do not
// close the output channel yourself.
func NewCirque[I any, O any](parallelism int64, processor func(I) O) (chan<- I, <-chan O) {
	inputChannel := make(chan I)
	outputChannel := make(chan O)

	inputHolder := NewSyncMap[int64, I]()
	outputHolder := NewSyncMap[int64, O]()

	// let the output goroutine know every time an input is processed, so it
	// can wake up and try to send outputs
	processCompletionSignal := make(chan struct{})

	// apply backpressure to make sure we're processing inputs only when outputs are
	// actually being collected - otherwise we're going to fill up memory with processed
	// jobs that aren't being taken out.
	outputBackpressureSignal := make(chan struct{}, parallelism)

	go func() { // process inputs
		inflightInputs := sync.WaitGroup{}
		inputPool := make(chan int64)

		// Start worker pool of specified size
		for n := int64(0); n < parallelism; n++ {
			inflightInputs.Add(1)
			go func() {
				for index := range inputPool {
					input, _ := inputHolder.Get(index)
					outputHolder.Set(index, processor(input))
					inputHolder.Delete(index)
					processCompletionSignal <- struct{}{}
				}
				inflightInputs.Done()
			}()
		}

		index := int64(0)
		for input := range inputChannel {
			inputHolder.Set(index, input)
			inputPool <- index
			index++
			outputBackpressureSignal <- struct{}{}
		}
		close(inputPool)

		inflightInputs.Wait()
		close(processCompletionSignal)
	}()

	go func() { // send outputs in order
		nextIndex := int64(0)
		for range processCompletionSignal {
			for true {
				if output, ok := outputHolder.Get(nextIndex); ok {
					outputChannel <- output
					outputHolder.Delete(nextIndex)
					nextIndex++
					<-outputBackpressureSignal
				} else {
					break
				}
			}
		}
		close(outputChannel)
	}()

	return inputChannel, outputChannel
}
