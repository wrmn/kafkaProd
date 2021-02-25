package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/mux"
)

func main() {
	logSetup()
	rand.Seed(time.Now().UnixNano())

	router := server()

	log.Fatal(http.ListenAndServe(":6010", router))
}

func logSetup() {
	file, err := os.OpenFile("log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)

	if err != nil {
		log.Fatal("Found error in log ", err)
	}

	log.SetOutput(file)
}

func server() *mux.Router {
	r := mux.NewRouter()

	r.HandleFunc("/", send).Methods("POST")

	return r
}

func send(w http.ResponseWriter, r *http.Request) {

	//wg.Wait()
	var wg sync.WaitGroup
	b, _ := ioutil.ReadAll(r.Body)
	wg.Add(1)
	//nice := rand.Intn(3000)
	//time.Sleep(time.Duration(nice) * time.Millisecond)
	//duration, _ := strconv.Atoi(string(b))

	//sum := fmt.Sprintf("response %dms to :%s with total %d", nice, string(b), nice+duration)

	//doProduce(broker, topic1, strconv.Itoa(duration+nice))

	go doProduce(broker, topic1, string(b), &wg)

	wg.Wait()
	sum, _ := doConsume()

	w.Write([]byte(sum))
}

func doProduce(broker string, topic string, msg string, wg *sync.WaitGroup) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})

	if err != nil {
		panic(err)
	}

	defer producer.Close()

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic1, Partition: kafka.PartitionAny},
		Value:          []byte(msg),
	}, nil)

	producer.Flush(15 * 1000)
	wg.Done()
}

//func doConsume(topic string, value chan string) {

//fmt.Printf("Starting consumer\n")

//consumerConf := kafka.ConfigMap{
//"bootstrap.servers":    broker,
//"group.id":             group,
//"auto.offset.reset":    "latest",
//"enable.partition.eof": true,
//}

//consumer, err := kafka.NewConsumer(&consumerConf)

//if err != nil {
//fmt.Println(err.Error())
//}

//defer consumer.Close()

//consumer.SubscribeTopics([]string{topic2}, nil)

//for {
//msg, err := consumer.ReadMessage(-1)
//if err == nil {
//rawmsg := string(msg.Value)
//fmt.Print(rawmsg)
//value <- rawmsg
//} else {
//fmt.Println(err.Error())
//}
//}
//}

func doConsume() (string, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":    "localhost:9092",
		"group.id":             group,
		"auto.offset.reset":    "latest",
		"enable.partition.eof": true,
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{topic2}, nil)

	fmt.Println("this consumer is runnning")

	for {
		fmt.Println("in looping")
		msg, err := c.ReadMessage(-1)
		fmt.Print(err == nil)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			rawmsg := string(msg.Value)
			return rawmsg, nil
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			return string(msg.Value), err
		}
	}
}
