package cirque

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

func NewCirque(parallelism int64, fun func(interface{}) interface{}) (chan<- interface{}, <-chan interface{}) {
	in := make(chan interface{})
	out := make(chan interface{})
	results := sync.Map{}
	done := make(chan struct{})
	popSignal := sync.NewCond(&sync.Mutex{})
	pushSignal := sync.NewCond(&sync.Mutex{})
	wg := sync.WaitGroup{}
	var leadingIndex int64 = 0
	var followingIndex int64 = 0
	go func() {
		for e := range in {
			wg.Add(1)
			popSignal.L.Lock()
			for (atomic.LoadInt64(&leadingIndex) - atomic.LoadInt64(&followingIndex)) >= parallelism {
				popSignal.Wait()
			}
			popSignal.L.Unlock()
			go func(current int64, element interface{}) {
				result := fun(element)
				log.Println("STORING ", current)
				results.Store(current, result)
				pushSignal.Broadcast()
			}(leadingIndex, e)
			atomic.AddInt64(&leadingIndex, 1)
		}
		wg.Wait()
		done <- struct{}{}
	}()
	go func() {
		for {
			select {
			case <-time.After(time.Duration(0)):
				pushSignal.L.Lock()
				log.Println("CHECKING ", followingIndex)
				log.Println("LEADING ", atomic.LoadInt64(&leadingIndex))
				if value, ok := results.Load(followingIndex); ok {
					log.Println("SENDING ", followingIndex)
					out <- value
					results.Delete(followingIndex)
					atomic.AddInt64(&followingIndex, 1)
					wg.Done()
					popSignal.Broadcast()
				} else {
					if atomic.LoadInt64(&leadingIndex) > atomic.LoadInt64(&followingIndex) {
						pushSignal.Wait()
					}
				}
				pushSignal.L.Unlock()
			case <-done:
				close(out)
				return
			}
		}
	}()
	return in, out
}
