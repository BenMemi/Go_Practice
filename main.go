package main

import (
	"fmt"
	"time"

	"github.com/BenMemi/Go_Practice/apple"
	"github.com/BenMemi/Go_Practice/sour"
)

var (
	messages = make(chan int)
)

func fibonaci(i int) (ret int) {
	if i == 0 {
		return 0
	}
	if i == 1 {
		return 1
	}
	return fibonaci(i-1) + fibonaci(i-2)
}

func randomInt(i int) {

	messages <- i

}

func add(a, b int) int {
	return a + b
}

func addThenSubtract(a, b int) int {
	return add(a, b) + apple.Fubtract(a, b)
}

func addThenSquare(a, b int) int {
	return sour.Square(add(a, b))
}

func main() {

	var i int
	for i = 0; i < 10; i++ {
		fmt.Println(fibonaci(i))
	}
	time.Sleep(5 * time.Second)
	for i = 0; i < 10; i++ {
		go randomInt(i)
		fmt.Println(<-messages)
	}
	defer close(messages)

}
