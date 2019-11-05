package cirque

import (
	"sync"
	"sync/atomic"
	"time"
)

func NewCirque(parallelism int64, processor func(interface{}) interface{}) (chan<- interface{}, <-chan interface{}) {
	input := make(chan interface{})
	output := make(chan interface{})

	stagedResults := sync.Map{}
	inFlight := sync.WaitGroup{}
	popSignal := sync.NewCond(&sync.Mutex{})
	pushSignal := sync.NewCond(&sync.Mutex{})
	finished := make(chan struct{})

	var leadingIndex int64 = 0
	var followingIndex int64 = 0

	go func() {
		popSignal.L.Lock()
		defer popSignal.L.Unlock()

		for element := range input {
			inFlight.Add(1)
			for (aR(&leadingIndex) - aR(&followingIndex)) >= parallelism {
				popSignal.Wait() // parallelism limit reached, wait until a pop happens
			}
			go func(current int64, currentElement interface{}) {
				stagedResults.Store(current, processor(currentElement))
				pushSignal.Broadcast() // store the result and broadcast a push event
			}(leadingIndex, element)
			aInc(&leadingIndex)
		}

		inFlight.Wait()
		finished <- struct{}{}
	}()

	go func() {
		pushSignal.L.Lock()
		defer pushSignal.L.Unlock()

		for {
			select {
			case <-time.After(time.Duration(0)): // effectively an infinite loop, relies on the push event wait for holding
				if value, ok := stagedResults.Load(followingIndex); ok {
					output <- value

					go stagedResults.Delete(followingIndex)
					go inFlight.Done()
					go popSignal.Broadcast()

					aInc(&followingIndex)

				} else {
					if aR(&leadingIndex) > aR(&followingIndex) { // wait for a push event if we still have work in progress
						pushSignal.Wait()
					}
				}
			case <-finished:
				close(output)
				return
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
