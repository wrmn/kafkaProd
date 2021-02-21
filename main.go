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

type CardAcceptorData struct {
	CardAcceptorTerminalId  string `json:"cardAcceptorTerminalID"`
	CardAcceptorName        string `json:"cardAcceptorName"`
	CardAcceptorCity        string `json:"cardAcceptorCity"`
	CardAcceptorCountryCode string `json:"cardAcceptorCountryCode"`
}

type Spec struct {
	Pan                           string           `json:"pan"`
	ProcessingCode                string           `json:"processingCode"`
	TotalAmount                   int              `json:"totalAmount"`
	AcquirerId                    string           `json:"acquirerID"`
	IssuerId                      string           `json:"issuerId"`
	TransmissionDateTime          string           `json:"transmissionDateTime"`
	LocalTransactionTime          string           `json:"localTransactionTime"`
	LocalTransactionDate          string           `json:"localTransactionDate"`
	ExpirationDate                string           `json:"expirationDate"`
	SettlementDate                string           `json:"settlementDate"`
	MerchantType                  string           `json:"merchantType"`
	CaptureDate                   string           `json:"captureDate"`
	AdditionalData                string           `json:"additionalData"`
	AdditionalDataVariable        string           `json:"additionalDataVariable"`
	AdditionalAmount              string           `json:"additionalAmount"`
	AdditionalData2               string           `json:"additionalData2"`
	Stan                          string           `json:"stan"`
	Refnum                        string           `json:"refnum"`
	AuthIdentificationNumber      string           `json:"authIdentificationNumber"`
	Currency                      string           `json:"currency"`
	PersonalIdentificationNumber  string           `json:"personalIdentificationNumber"`
	TerminalID                    string           `json:"terminalID"`
	TerminalData                  string           `json:"terminalData"`
	TokenData                     string           `json:"tokenData"`
	AccountFrom                   string           `json:"accountFrom"`
	AccountTo                     string           `json:"accountTo"`
	CategoryCode                  string           `json:"categoryCode"`
	SettlementAmount              string           `json:"settlementAmount"`
	CardholderBillingAmount       string           `json:"cardholderBillingAmount"`
	SettlementConversionRate      string           `json:"settlementConversionRate"`
	CardHolderBillingConvRate     string           `json:"cardHolderBillingConvRate"`
	PointOfServiceEntryMode       string           `json:"pointOfServiceEntryMode"`
	AuthIDResponseLength          string           `json:"authIDResponseLength"`
	Track2Data                    string           `json:"track2Data"`
	CardAcceptorID                string           `json:"cardAcceptorID"`
	CardAcceptorData              CardAcceptorData `json:"cardAcceptorData"`
	SettlementCurrencyCode        string           `json:"settlementCurrencyCode"`
	CardHolderBillingCurrencyCode string           `json:"cardHolderBillingCurrencyCode"`
	AdditionalDataNational        string           `json:"additionalDataNational"`
	AdditionalResponseData        string           `json:"additionalResponseData"`
	ReceivingInstitutionIDCode    string           `json:"receivingInstitutionIDCode"`
}

type Response struct {
	ResponseCode        int    `json:"responseCode"`
	ReasonCode          int    `json:"reasonCode"`
	ResponseDescription string `json:"responseDescription"`
}

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
