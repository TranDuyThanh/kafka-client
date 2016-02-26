package main

import (
	"fmt"
	"sync"
	"time"

	// "github.com/TranDuyThanh/go-spew/spew"
	"github.com/TranDuyThanh/ini"
	"github.com/TranDuyThanh/kafka-client"
)

func init() {

}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	cfg, err := ini.Load("example/config.ini")
	check(err)
	brokerList := cfg.Section("kafka").Key("BROKER_LIST").String()
	kafkaClient := kafka.Init(brokerList)
	// spew.Dump(kafkaClient)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		fmt.Println("Start consumer...")
		kafkaClient.Consumer.ConsumeMessage("booking_success", bar, 1, 2, 3)
		wg.Done()
	}()

	time.Sleep(3 * time.Second)

	topic := "booking_success"
	value := "Hello world"
	kafkaClient.Producer.ProduceMessage(topic, value)

	wg.Wait()
}

func bar(a, b, c int, msgFromConsumer string) {
	fmt.Println("callback:", a, b, c, msgFromConsumer)
}
