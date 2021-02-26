package main

import "sync"

var (
	broker = "localhost:9092"
	group  = "thigSroup"
	topic1 = "channelKafkax"
	topic2 = "kafkaBillerx"

	msgChan chan string
	wg      sync.WaitGroup
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
