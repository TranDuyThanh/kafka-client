package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/TranDuyThanh/go-spew/spew"
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
	spew.Dump(kafkaClient)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		fmt.Println("Start consumer 1...")
		kafkaClient.Consumer.ConsumeMessage("topic_1", bar, 1, 1, 1)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		fmt.Println("Start consumer 2...")
		kafkaClient.Consumer.ConsumeMessage("topic_2", bar, 2, 2, 2)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		fmt.Println("Start consumer 3...")
		kafkaClient.Consumer.ConsumeMessage("topic_3", bar, 3, 3, 3)
		wg.Done()
	}()

	time.Sleep(2 * time.Second)

	for i := 0; i < 50; i++ {
		message := fmt.Sprintf("message %d", i+1)
		kafkaClient.Producer.ProduceMessage("topic_1", message)
	}

	wg.Wait()
}

func bar(a, b, c int, msgFromConsumer string) {
	time.Sleep(5000 * time.Millisecond)
	fmt.Println("callback:", a, b, c, msgFromConsumer)
}
