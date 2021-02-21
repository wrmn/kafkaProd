package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/mux"
)

var (
	broker = "localhost:9092"
	group  = "channel"
	topic1 = "channelKafka2"
	topic2 = "kafkaBiller2"
)

func main() {
	go logSetup()

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

	r.HandleFunc("/payment/json", sendJSON).Methods("POST")

	return r
}

func sendJSON(w http.ResponseWriter, r *http.Request) {
	b, _ := ioutil.ReadAll(r.Body)

	go doProduce(broker, topic1, string(b))
}

func doProduce(broker string, topic string, msg string) {
	log.Println(msg)

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}

	defer p.Close()
	go func() {
		for e := range p.Events() {
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
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic1, Partition: kafka.PartitionAny},
		Value:          []byte(msg),
	}, nil)
	p.Flush(15 * 1000)

}
