package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

func main() {
	var wg sync.WaitGroup
	rand.Seed(time.Now().UnixNano())

	i := 0
	for i < 10 {
		wg.Add(1)

		go func(wg *sync.WaitGroup, num int) {
			nice := rand.Intn(3000)
			time.Sleep(time.Duration(nice) * time.Millisecond)

			fmt.Printf("wait %dms for msg no %d \n", nice, num)
			request(nice)
			wg.Done()
		}(&wg, i)
		i++
	}

	wg.Wait()
	fmt.Println("Done testing!")

}

func request(num int) {
	client := &http.Client{}

	req, err := http.NewRequest("POST", "http://localhost:6010", bytes.NewBuffer([]byte(strconv.Itoa(num))))
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		log.Fatalf("Failed to sent request to https://tiruan.herokuapp.com/biller. Error: %v\n", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Failed to get response from https://tiruan.herokuapp.com/biller. Error: %v\n", err)
	}

	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	log.Printf("%s \n", body)
}
