package cirque

import (
	"sync"
	"sync/atomic"
)

func NewCirque(parallelism int64, processor func(interface{}) interface{}) (chan<- interface{}, <-chan interface{}) {
	input := make(chan interface{})
	output := make(chan interface{})

	stagedResults := sync.Map{}
	popSignal := sync.NewCond(&sync.Mutex{})
	pushSignal := sync.NewCond(&sync.Mutex{})

	var completeFlag int64 = 0 // ideally a boolean, but we want an atomic variable without typecasts
	var leadingIndex int64 = 0
	var followingIndex int64 = 0

	go func() { // process all the inputs
		popSignal.L.Lock()
		defer popSignal.L.Unlock()

		for element := range input {
			for (aR(&leadingIndex) - aR(&followingIndex)) >= parallelism {
				popSignal.Wait() // parallelism limit reached, wait until a pop happens
			}
			go func(current int64, currentElement interface{}) {
				stagedResults.Store(current, processor(currentElement))
				pushSignal.Broadcast() // store the result and broadcast a push event
			}(leadingIndex, element)
			aInc(&leadingIndex)
		}
		aInc(&completeFlag)
	}()

	go func() { // send outputs in order
		pushSignal.L.Lock()
		defer pushSignal.L.Unlock()

		for { // infinite loop that relies on the push event wait for holding
			if aR(&completeFlag) > 0 && aR(&followingIndex) == aR(&leadingIndex) {
				// all inputs have been accepted and we've caught up on outputs
				close(output)
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

func aInc(i *int64) int64 {
	return atomic.AddInt64(i, 1)
}

func aR(i *int64) int64 {
	return atomic.LoadInt64(i)
}
