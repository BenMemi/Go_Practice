package main

import (
	"fmt"

	"github.com/BenMemi/Go_Practice/apple"
	"github.com/BenMemi/Go_Practice/sour"
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
		fmt.Printf("%d ", fibonaci(i))
	}
}
