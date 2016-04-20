package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/TranDuyThanh/go-spew/spew"
	"github.com/TranDuyThanh/ini"

	"github.com/TranDuyThanh/kafka-client"
)

func main() {

	cfg, err := ini.Load("example/config.ini")
	check(err)
	brokerList := cfg.Section("kafka").Key("BROKER_LIST").String()
	kafkaClient := kafka.Init(brokerList)
	spew.Dump(kafkaClient)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		fmt.Println("Start consumer group 1")
		kafkaClient.ConsumerGroup.ConsumeMessage("test", "sss", hello)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		fmt.Println("Start consumer group 2")
		kafkaClient.ConsumerGroup.ConsumeMessage("test", "sss", hello)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		fmt.Println("Start consumer group 3")
		kafkaClient.ConsumerGroup.ConsumeMessage("test", "sss", hello)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		fmt.Println("Start consumer group 4")
		kafkaClient.ConsumerGroup.ConsumeMessage("test", "sss", hello)
		wg.Done()
	}()

	wg.Wait()
}

func hello(msg string) {
	time.Sleep(5000 * time.Millisecond)
	fmt.Println(time.Now(), msg)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
