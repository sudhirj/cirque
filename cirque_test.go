package cirque

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testCase struct {
	input          []int
	expectedOutput []int
	description    string
}

var cases = []testCase{
	{
		input:          []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		expectedOutput: []int{2, 4, 6, 8, 10, 12, 14, 16, 18, 20},
		description:    "Computation check",
	},
	{
		input:          []int{},
		expectedOutput: []int{},
		description:    "Empty Channel check",
	},
}

func TestCirque(t *testing.T) {
	var wg sync.WaitGroup
	for iter := 0; iter < 100; iter++ {
		for _, c := range cases {
			wg.Add(1)
			go func(cs testCase) {
				defer wg.Done()
				var measuredParallelism int64 = 0
				var wipCount int64 = 0

				var maxParallelism int64 = 3
				inputChannel, outputChannel := NewCirque(maxParallelism, func(i int) int {
					atomic.AddInt64(&measuredParallelism, 1)
					time.Sleep(time.Duration(rand.Int63n(10)) * time.Millisecond)
					atomic.AddInt64(&measuredParallelism, -1)
					return i * 2
				})

				go func() {
					for _, i := range cs.input {
						atomic.AddInt64(&wipCount, 1)

						inputChannel <- i

						if atomic.LoadInt64(&measuredParallelism) > maxParallelism {
							t.Error("SO MUCH CANNOT ABLE TO HANDLE!")
						}
						if atomic.LoadInt64(&wipCount) > maxParallelism*2 {
							// Twice max parallelism because we can have jobs running, and the same number waiting for output transfer
							t.Error("NO BACKPRESSURE!", maxParallelism, atomic.LoadInt64(&wipCount))
						}
					}
					close(inputChannel)
				}()

				var actualOutput []int
				for i := range outputChannel {
					atomic.AddInt64(&wipCount, -1)
					actualOutput = append(actualOutput, i)
				}
				if len(cs.expectedOutput) > 0 && !reflect.DeepEqual(cs.expectedOutput, actualOutput) {
					t.Errorf("WRONG WRONG WRONG. Case: %s \n Expected: %v, Actual: %v,",
						cs.description, cs.expectedOutput, actualOutput)
				}
			}(c)
		}
		wg.Wait()
	}
}
func ExampleNewCirque() {
	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	inputChannel, outputChannel := NewCirque(3, func(i int) int {
		time.Sleep(time.Duration(rand.Int63n(100)) * time.Millisecond)
		return i * 2
	})

	go func() {
		for _, i := range inputs {
			inputChannel <- i
		}
		close(inputChannel)
	}()

	var output []int
	for i := range outputChannel {
		output = append(output, i)
	}
	fmt.Println(output)

	// Output: [2 4 6 8 10 12 14 16 18 20]
}
