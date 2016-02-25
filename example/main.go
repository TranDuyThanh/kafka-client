package main

import (
	"fmt"
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
	kafkaClient := kafka.Init(cfg.Section("kafka").Key("BROKER_LIST").String())
	// spew.Dump(kafkaClient)

	go consumerTask(kafkaClient)
	time.Sleep(3 * time.Second)

	topic := "booking_success"
	value := "Hello world"
	kafkaClient.Producer.ProduceMessage(topic, value)

	time.Sleep(2 * time.Second)
}

func consumerTask(kafkaClient *kafka.Kafka) {
	fmt.Println("Start consumer...")
	kafkaClient.Consumer.ConsumeMessage("booking_success", bar, 1, 2, 3)
}

func bar(a, b, c int) {
	fmt.Println("callback:", a, b, c)
}
