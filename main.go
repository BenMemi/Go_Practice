package main

import (
	"fmt"
	"math/rand"
	"time"
)

// Operation requests
// When a read/write is wanted a request must be sent to the broker and the response will be sent to a provided channel
// its safe to use channel here as requests can be rid of when processed

type Consumer struct {
	id                      int
	data_channel            chan int
	acknowledgement_channel chan bool
	topic                   topic
	offset                  int
}

type Producer struct {
	id                      int
	topic                   topic
	acknowledgement_channel chan bool
}

// allows you to also start a topic with some data allready in it which is nice
type topic struct {
	name string
	//partitions int
	//replicas int
	log          map[int]int
	length       int
	writeChannel chan int
	//deleteChannel           chan bool
	registerProducerChannel chan registerProducerRequest
	registerConsumerChannel chan subscribeRequest
	producers               []Producer
	consumers               []Consumer
}

type Broker struct {
	consumers []Consumer
	producers []Producer
	topics    []topic
	id        int
}

// todo these are the same struct, can just be one
// consumer makes a request to start reading from a topic
type subscribeRequest struct {
	consumer           Consumer
	topic_registration chan bool
}

type registerProducerRequest struct {
	producer Producer
	//can be extended to cases where the application is moved to a different broker
	broker_registration chan bool
	//topic registration is the most crucial as it tells if the producer can work or not
	topic_registration chan bool
}

// make a request to start producing to a topic

var (
	//channel to send requests to broker to start a producer on a topic
	producerRequests = make(chan registerProducerRequest)
	//channel to send requests to broker to start a consumer on a topic
	subscribeRequests = make(chan subscribeRequest)
	//channel to send to when a new topic is to be created
	createTopicRequests = make(chan topic)
)

// Topic manager ensures that every read happens in some order and each topic can be written to withotu blocking another topics writes
// This is blocking for writes but I don't know a solution to make writing to a topic non blocking
// Would love some advise here
// Fan in pattern here
func TopicManager(topic topic) {
	fmt.Println("Topic manager started for topic: ", topic.name)
	for {
		select {
		//actually this should be a new process when a producer/consumer is registered that listens to that producer/consumer specifically and handles stuff for it
		//although writes can still really only happen one at a time, I don't see how that can be worked around
		case message := <-topic.writeChannel:
			topic.log[topic.length] = message
			topic.length++
			fmt.Println("Topic: ", topic.name, " has been written to with message: ", message, "record number: ", topic.length)
			fmt.Println("Topic: ", topic.name, " has: ", topic.length, " messages")
			//cannot send to consumer before the message is written but this is logically to me
			//I think go handles this fine even when there are no consumers?
			for _, consumer := range topic.consumers {
				//NOTE: no gaurentee order of messages consumer will get TODO: fix this
				//Possible fix is to include the index of the message in the message and the consumer can re-org
				//as needed from that index
				go sendToConsumer(message, consumer)
			}
			//TODO: then start a process to send the message to all consumers
			//Issue here that I see is that consumers that are newly registered won't get the message, only ones that are registered at the time of the write
			//Also race condition here between consumer being registered an a new consumer being added
		case registerProducerRequest := <-topic.registerProducerChannel:
			fmt.Println("Producer: ", registerProducerRequest.producer.id, " registered on topic: ", topic.name)
			topic.producers = append(topic.producers, registerProducerRequest.producer)
			fmt.Println("Sending message to producer to acknowledge registration. Producer ID: ", registerProducerRequest.producer.id)
			registerProducerRequest.topic_registration <- true
		case subscribeRequest := <-topic.registerConsumerChannel:
			fmt.Println("Consumer: ", subscribeRequest.consumer.id, " registered on topic: ", topic.name)
			topic.consumers = append(topic.consumers, subscribeRequest.consumer)
			fmt.Println("Sending message to consumer to acknowledge registration. Consumer ID: ", subscribeRequest.consumer.id)
			subscribeRequest.topic_registration <- true
			//TODO Case here to fill up a topic with old data from the topic
			//I forsee race conditions here I think

		}

	}

}

func sendToConsumer(message int, consumer Consumer) {
	consumer.data_channel <- message
	aknownledged := false
	for !aknownledged {
		select {
		//TODO: this channel should be unique to each message? or this function exists when another message is sucessfully recieved
		//because the channel is common across all messages
		case aknownledged = <-consumer.acknowledgement_channel:
			if aknownledged {
				fmt.Println("Consumer: ", consumer.id, " has acknowledged message: ", message)
			} else {
				fmt.Println("Consumer: ", consumer.id, " has failed to acknowledge message: ", message)
			}
		}
	}
	fmt.Println("Consumer acknowledged message: ", message, "killing sendToConsumer process")
	//if aknowledged kill the routine
}

func CreateProducer(topic topic, id int, speed int) {
	broker_registration := make(chan bool)
	topic_registration := make(chan bool)
	Producer := Producer{
		id:                      id,
		topic:                   topic,
		acknowledgement_channel: make(chan bool),
	}

	//Unnescessary stuff here TODO: Clean up
	producerRequests <- registerProducerRequest{
		producer:            Producer,
		broker_registration: broker_registration,
		topic_registration:  topic_registration,
	}
	topic.registerProducerChannel <- registerProducerRequest{
		producer:            Producer,
		broker_registration: broker_registration,
		topic_registration:  topic_registration,
	}

	//wait for feedback on whether registration was successful
	//TODO reformat this with consumer logic
	for {
		select {
		case topic_registration_state := <-topic_registration:
			if topic_registration_state {
				fmt.Println("Producer: ", id, " registered successfully on topic: ", topic.name)
				for i := 0; i < 10; i++ {
					topic.writeChannel <- i
					//wait some time to give topic manager a chance to process other things
					time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
					time.Sleep(time.Second * time.Duration(speed))
				}
				return
			} else {
				fmt.Println("Producer: ", id, " failed to register on topic: ", topic.name)
				return
			}
		}
	}
}

// create a consumer to read froma  certain offset of a topic
// Speed here is to simulate a slow consumer (one that does something more heavy with messages)
// and to verify that one consumer does not block the other)
func createConsumer(topic topic, id int, speed int, offset ...int) {
	//if no offset is provuded then default to 0
	if len(offset) == 0 {
		offset = append(offset, 0)
	}
	topic_registration := make(chan bool)
	Consumer := Consumer{
		id:                      id,
		data_channel:            make(chan int),
		acknowledgement_channel: make(chan bool),
		topic:                   topic,
		offset:                  offset[0],
	}
	subscribeRequests <- subscribeRequest{
		consumer:           Consumer,
		topic_registration: topic_registration,
	}
	topic.registerConsumerChannel <- subscribeRequest{
		consumer:           Consumer,
		topic_registration: topic_registration,
	}
	//Check if the consumer has successfully been registered on the broker and topic manager
	topic_registration_state := false
	for !topic_registration_state {
		select {
		case topic_registration_state = <-topic_registration:
			if topic_registration_state {
				fmt.Println("Consumer: ", id, " registered successfully on topic: ", topic.name)
				close(topic_registration)
			} else {
				//TODO: Better error handling here, if a false is recieved then the consumer should probably crash
				fmt.Println("Consumer: ", id, " failed to register on topic: ", topic.name, " retrying")
			}
		}
	}
	for {
		//pull message from channel when data is available
		message := <-Consumer.data_channel
		fmt.Println("Consumer: ", id, " has received message: ", message)
		//send an acknowledgement to the topic manager
		Consumer.acknowledgement_channel <- true
		time.Sleep(time.Second * time.Duration(speed))
	}
}

//Produce should be a seperate function but for MVP purposes CreateProducer will just send some stuff on its own
// func Produce(message int) (int) {
// 	return message
// }

// TODO: You should be able to start brokers and they should be able to communicate and load balance across multiple brokers
// but this is out of scope
// Broker is a meta process manager, it starts up topic managers and keeps a record of consumers, topics and producers
// This record is not really used here but could be extended to allow for load balancing between brokers
func broker(id int) {
	//make a broker instance
	broker := Broker{
		consumers: []Consumer{},
		producers: []Producer{},
		topics:    []topic{},
		id:        id,
	}

	for {
		select {
		//this should actually be sent to some "broker manager" that determines which broker will manage the topic based on load
		//I believe in kafka the leader deals with this but not in this way exactly
		//This is more of an example of how load balancing could be done between brokers
		case topic := <-createTopicRequests:
			//idea here is that topic managers are created as nescessary but potentially brokers could talk to each other and boot
			//topic managers for eachother to balance load by transfering topics to manage to eachother/booting topic managers for eachother
			//while appending to the another brokers storage
			//This is no where implemented here but just a thought
			//This load balancing wouldn't punish "light" topics for load of "heavy" topics and would let you vertically scale across a horizontal system of brokers
			//This really should be more of a partition manager I think (or whatever the smallest component of a topic is that can be atmoically managed by a process)
			broker.topics = append(broker.topics, topic)
			fmt.Println("Created topic: ", topic.name)
			fmt.Println("There are now: ", len(broker.topics), " topics")
			//TODO: for particularly popular/large topics we could boot up seperate/more topic managers for a single topic
			go TopicManager(topic)
			continue
		case subscribeRequest := <-subscribeRequests:
			//add the consumer to the broker
			broker.consumers = append(broker.consumers, subscribeRequest.consumer)
			fmt.Println("Added consumer: ", subscribeRequest.consumer.id)
			fmt.Println("There are now: ", len(broker.consumers), " consumers")
			//ConsumerChannels = make([]reflect.SelectCase, len(broker.consumers))
		case newProducerRequest := <-producerRequests:
			//add the producer to the broker
			broker.producers = append(broker.producers, newProducerRequest.producer)
			fmt.Println("Added producer: ", newProducerRequest.producer.id)
			fmt.Println("There are now: ", len(broker.producers), " producers on broker: ", broker.id)
			continue
			//ProducerChannels = make([]reflect.SelectCase, len(broker.producers))

		}
	}
}

//Should have a function for creating topics instead of calling it in main
//func CreateTopic ()

func main() {
	//start the broker with id(1)
	go broker(1)

	//make a second topic to ensure broker is retaining state correctly

	testTopic2 := topic{
		name: "test2",
		//partitions: 1,
		//replicas:   1,
		log:                     map[int]int{},
		length:                  0,
		writeChannel:            make(chan int),
		registerProducerChannel: make(chan registerProducerRequest),
		producers:               []Producer{},
		consumers:               []Consumer{},
		registerConsumerChannel: make(chan subscribeRequest),
	}
	createTopicRequests <- testTopic2
	go createConsumer(testTopic2, 1, 0)
	go CreateProducer(testTopic2, 1, 0)
	go createConsumer(testTopic2, 2, 3)
	go CreateProducer(testTopic2, 2, 1)
	time.Sleep(time.Second * 1)
	go createConsumer(testTopic2, 3, 0)
	time.Sleep(time.Second * 2)
	go CreateProducer(testTopic2, 3, 2)
	time.Sleep(time.Second * 1)
	go createConsumer(testTopic2, 4, 0)

	//sleep so go routine does not die before anything happens
	time.Sleep(time.Second * 50)
}
