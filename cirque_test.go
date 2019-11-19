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
	Input          []int
	ExpectedOutput []int
	Desc           string
}

var cases = []testCase{
	{
		Input:          []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		ExpectedOutput: []int{2, 4, 6, 8, 10, 12, 14, 16, 18, 20},
		Desc:           "Computation check",
	},
	{
		Input:          []int{},
		ExpectedOutput: []int{},
		Desc:           "Empty Channel check",
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

				var maxParallelism int64 = 3
				inputChannel, outputChannel := NewCirque(maxParallelism, func(i interface{}) interface{} {
					atomic.AddInt64(&measuredParallelism, 1)
					time.Sleep(time.Duration(rand.Int63n(10)) * time.Millisecond)
					atomic.AddInt64(&measuredParallelism, -1)
					return i.(int) * 2
				})

				go func() {
					for _, i := range cs.Input {
						inputChannel <- i
						if atomic.LoadInt64(&measuredParallelism) > maxParallelism {
							t.Error("SO MUCH CANNOT ABLE TO HANDLE!")
						}
					}
					close(inputChannel)
				}()

				var actualOutput []int
				for i := range outputChannel {
					actualOutput = append(actualOutput, i.(int))
				}
				if len(cs.ExpectedOutput) > 0 && !reflect.DeepEqual(cs.ExpectedOutput, actualOutput) {
					t.Errorf("WRONG WRONG WRONG. Case: %s \n Expected: %v, Actual: %v,",
						cs.Desc, cs.ExpectedOutput, actualOutput)
				}
			}(c)
	}
	wg.Wait()
}

func ExampleNewCirque() {
	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	inputChannel, outputChannel := NewCirque(3, func(i interface{}) interface{} {
		time.Sleep(time.Duration(rand.Int63n(100)) * time.Millisecond)
		return i.(int) * 2
	})

	go func() {
		for _, i := range inputs {
			inputChannel <- i
		}
		close(inputChannel)
	}()

	var output []int
	for i := range outputChannel {
		output = append(output, i.(int))
	}
	fmt.Println(output)

	// Output: [2 4 6 8 10 12 14 16 18 20]
}
