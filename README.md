# Cirque
A circular queue that processes jobs in parallel but returns results in FIFO.

```go
inputs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

done := make(chan bool)
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

go func() {
    var actualOutput []int
    for i := range outputChannel {
        actualOutput = append(actualOutput, i.(int))
    }
    fmt.Println(actualOutput)
    done <- true
}()

<-done
// Output: [2 4 6 8 10 12 14 16 18 20]
```
