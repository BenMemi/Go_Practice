package main

import (
	"fmt"
	"math/rand"
	"time"
)

//Note; throughout a lot of code it would've been better to pass out pointer references to messages/etc instead of copying them
//when processing reads (consumer requests)

type Consumer struct {
	id               int
	data_channel     chan Message
	topic            topic
	offset           int
	archive          bool
	archive_data     chan Message
	archive_complete chan bool
	archive_request  chan bool
	archive_goal     chan int
}

type Message struct {
	value        int
	index        int
	acknowledged chan bool
}

type Producer struct {
	id                      int
	topic                   topic
	acknowledgement_channel chan bool
}

// allows you to also start a topic with some data already in it which is nice
type topic struct {
	name string
	//Variables that could be added later for scalability
	//partitions int
	//replicas int
	log          map[int]Message
	length       int
	writeChannel chan Message
	//could have a channel to delete a topic
	//deleteChannel           chan bool
	registerProducerChannel chan registerProducerRequest
	registerConsumerChannel chan subscribeRequest
	archive_request         chan Consumer
	producers               []Producer
	consumers               []Consumer
}

type Broker struct {
	consumers []Consumer
	producers []Producer
	topics    []topic
	id        int
}

// these are the same struct, could just be one
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

// Should be part of a struct instead of global. These channels should be variables of brokers/topic managers
var (
	//channel to send requests to broker to start a producer on a topic
	producerRequests = make(chan registerProducerRequest)
	//channel to send requests to broker to start a consumer on a topic
	subscribeRequests = make(chan subscribeRequest)
	//channel to send to when a new topic is to be created
	createTopicRequests = make(chan topic)
)

// Topic manager ensures that every read happens in some order and each topic can be written to without blocking another topics writes
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
			message.index = len(topic.log)
			topic.log[topic.length] = message
			topic.length++
			fmt.Println("Topic: ", topic.name, " has been written to with message: ", message, "message index: ", message.index)
			fmt.Println("Topic: ", topic.name, " has: ", topic.length, " messages")
			//cannot send to consumer before the message is written but this is logically to me
			//I think go handles this fine even when there are no consumers?
			message.acknowledged <- true
			for _, consumer := range topic.consumers {
				//NOTE: no guarantee order of messages consumer will get TODO: fix this
				//Possible fix is to include the index of the message in the message and the consumer can re-org
				//as needed from that index

				go sendToConsumer(consumer, message.value, message.index, false)
			}
			//Issue here that I see is that consumers that are newly registered won't get the message, only ones that are registered at the time of the write
			//Also race condition here between consumer being registered an a new consumer being added
			//To combat the above there is an archive stream functionality that will catch a consumer up on past messages on request
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
		case consumer := <-topic.archive_request:
			//conditional could be here to check if topic manager/broker has capacity to do this
			fmt.Println("Archived request received from consumer ", consumer.id, " for topic: ", topic.name)
			consumer.archive_request <- true
			//send most up to date log to consumer
			//I think this isn't efficient because log might be getting re-initiated here instead of being a reference?
			//of course we could just send the whole log in one go but that feels like cheating
			//and wouldn't scale as the log becomes larger
			go archive_stream(consumer, topic.log)

		}

	}

}

func archive_stream(consumer Consumer, log map[int]Message) {
	goal := len(log)
	consumer.archive_goal <- goal
	fmt.Println("goal is ", goal)
	for _, message := range log {

		go sendToConsumer(consumer, message.value, message.index, true)
	}
	complete := <-consumer.archive_complete
	if complete {
		fmt.Println("Archive complete")
	} else {
		fmt.Println("Archive failed")
	}
}

// could use interface instead of these types variables
func sendToConsumer(consumer Consumer, value int, index int, archive bool) {
	//make a copy of the message and send it to the consumer so your not just passing a reference
	//Could be better I think?
	message := Message{value, index, make(chan bool)}
	if archive {
		consumer.archive_data <- message
	} else {
		consumer.data_channel <- message
	}
	acknowledged := <-message.acknowledged
	if acknowledged {
		fmt.Println("Consumer: ", consumer.id, " has acknowledged message: ", message, " with index: ", index)
	} else {
		fmt.Println("Consumer: ", consumer.id, " has not acknowledged message: ", message, " with index: ", index)
	}
	//kill the routine
}

func CreateProducer(topic topic, id int, speed int) {
	broker_registration := make(chan bool)
	topic_registration := make(chan bool)
	Producer := Producer{
		id:                      id,
		topic:                   topic,
		acknowledgement_channel: make(chan bool),
	}

	//Unnecessary stuff here, should be redone
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
	for {
		select {
		case topic_registration_state := <-topic_registration:
			if topic_registration_state {
				fmt.Println("Producer: ", id, " registered successfully on topic: ", topic.name)
				for i := 0; i < 10; i++ {
					acknowledgment := make(chan bool)
					//here it is pointless to provide index because it assigned by the topic manager
					topic.writeChannel <- Message{i, 0, acknowledgment}
					acknowledged := <-acknowledgment
					if acknowledged {
						fmt.Println("Producer: ", id, " received feedback that message was successfully received by topic manager. Message: ", i)
					} else {
						fmt.Println("Message: ", i, " failed to be received by topic manager")
					}
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

// create a consumer to read from a  certain offset of a topic
// Speed here is to simulate a slow consumer (one that does something more heavy with messages)
// A feature missing here would be to add functionality for the consumer to re-organize messages when they are received in the incorrect order
// This would be fairly simple to do because each message has an index sent with it
func createConsumer(topic topic, id int, speed int, archive bool, offset ...int) {
	//if no offset is provided then default to 0
	if len(offset) == 0 {
		offset = append(offset, 0)
	}
	topic_registration := make(chan bool)

	Consumer := Consumer{
		id:               id,
		data_channel:     make(chan Message),
		topic:            topic,
		offset:           offset[0],
		archive:          archive,
		archive_data:     make(chan Message),
		archive_goal:     make(chan int),
		archive_complete: make(chan bool),
		archive_request:  make(chan bool),
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
				//TODO: Better error handling here, if a false is received then the consumer should probably crash
				fmt.Println("Consumer: ", id, " failed to register on topic: ", topic.name, " retrying")
			}
		}
	}
	if archive {
		fmt.Println("sending Archive Request to topic manager")
		topic.archive_request <- Consumer
		ArchiveRequestStatus := <-Consumer.archive_request
		fmt.Println("Archive Request Status: ", ArchiveRequestStatus)
		goal := <-Consumer.archive_goal
		if ArchiveRequestStatus {
			fmt.Println("Consumer: ", id, " has been approved for archive streaming")
			for {
				Message := <-Consumer.archive_data
				fmt.Println("Consumer: ", id, " has received message: ", Message, "with index: ", Message.index)
				Message.acknowledged <- true
				//Note: No guarantee that messages will be received in order but each message does have an index so its feasible for consumers
				//to re-order the messages themselves in memory/storage as needed. Here just iterating for simplicity
				Consumer.offset += 1

				if Consumer.offset == goal {
					fmt.Println("Consumer: ", id, " has completed archive streaming")
					Consumer.archive_complete <- true
					archive = false
					break
				} else {
				}
			}
		} else {
			fmt.Println("Consumer: ", id, " has not been approved for archive streaming")
			return
		}

	}
	fmt.Println("regular streaming has commenced for consumer: ", id)
	for {
		//pull message from channel when data is available
		message := <-Consumer.data_channel
		fmt.Println("Consumer: ", id, " has received message: ", message, "of index: ", message.index)
		//send acknowledgement to topic manager
		message.acknowledged <- true
		//send an acknowledgement to the topic manager

		time.Sleep(time.Second * time.Duration(speed))
	}
}

//Produce should be a separate function but for MVP purposes CreateProducer will just send some stuff on its own
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
			//idea here is that topic managers are created as necessary but potentially brokers could talk to each other and boot
			//topic managers for each other to balance load by transferring topics to manage to each-other/booting topic managers for each other
			//while appending to the another brokers storage
			//This is no where implemented here but just a thought
			//This load balancing wouldn't punish "light" topics for load of "heavy" topics and would let you vertically scale across a horizontal system of brokers
			//This really should be more of a partition manager I think (or whatever the smallest component of a topic is that can be atomically managed by a process)
			broker.topics = append(broker.topics, topic)
			fmt.Println("Created topic: ", topic.name)
			fmt.Println("There are now: ", len(broker.topics), " topics")
			//TODO: for particularly popular/large topics we could boot up separate/more topic managers for a single topic
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
		log:                     map[int]Message{},
		length:                  0, //unnecessary but for clarity
		writeChannel:            make(chan Message),
		registerProducerChannel: make(chan registerProducerRequest),
		producers:               []Producer{}, //unused currently but can be used for load balancing later
		consumers:               []Consumer{},
		registerConsumerChannel: make(chan subscribeRequest),
		archive_request:         make(chan Consumer),
	}
	createTopicRequests <- testTopic2
	go createConsumer(testTopic2, 1, 0, false)

	go CreateProducer(testTopic2, 1, 0)

	go createConsumer(testTopic2, 2, 3, false)

	go CreateProducer(testTopic2, 2, 0)

	time.Sleep(time.Second * 1)

	go createConsumer(testTopic2, 3, 2, true)

	//make a delay
	time.Sleep(time.Second * 2)

	go CreateProducer(testTopic2, 3, 2)

	time.Sleep(time.Second * 1)

	go createConsumer(testTopic2, 4, 0, true)

	time.Sleep(time.Second * 1)

	go CreateProducer(testTopic2, 4, 0)

	//sleep so go routine does not die before anything happens
	time.Sleep(time.Second * 50)

	//30 messages should be in topic by end
	//Producers should not block consumers and vice versa
	//Producers block eachother (writes only happen 1 at a time until partitions are implemented)
}
