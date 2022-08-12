package main

import (
	"fmt"
	"math/rand"
	"time"
)

// Operation requests
// When a read/write is wanted a request must be sent to the broker and the response will be sent to a provided channel
// its safe to use channel here as requests can be rid of when processed
type consumeRequest struct {
	key      int
	response chan int
}

// also for writes should send if write was successful or not
type produceRequest struct {
	key      int
	val      int
	response chan bool
}

// broker manages all states and is just a process which processes and waits for requests
// Q: I think this would still be blocking considering the broker has to process a request before it can process another
// I believe the response for each request should be processed async in a seperate thread allowing for non blocking processing

func broker() {
	var store = make(map[int]int)
	for {
		select {
		case read := <-readRequest:

			read.response <- store[read.key]
		case write := <-writeRequest:
			store[write.key] = write.val
			write.response <- true
		}
	}
}

// consumer makes read requests for a random integer key in the store mapping, waits a milisecond before trying again
func consumer(consumeChannel chan consumeRequest) {
	for {
		read := consumeRequest{
			key:      rand.Intn(100),
			response: make(chan int)}
		readRequest <- read
		x := <-read.response
		fmt.Println("read done: ", x)
		time.Sleep(time.Millisecond)
	}
}

func producer(produceChannel chan produceRequest, id int) {
	for {
		write := produceRequest{
			key:      rand.Intn(100),
			val:      rand.Intn(100),
			response: make(chan bool)}
		writeRequest <- write
		<-write.response
		fmt.Println("write done: ", write.val, write.key)
		fmt.Print("Producer: ", id, " ")
		time.Sleep(time.Millisecond)
	}
}

var (
	readRequest  = make(chan consumeRequest)
	writeRequest = make(chan produceRequest)
)

func main() {

	go broker()

	for r := 0; r < 100; r++ {
		go consumer(readRequest)
	}

	for w := 0; w < 100; w++ {
		go producer(writeRequest, w)
	}

	time.Sleep(time.Second)

}
