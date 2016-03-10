package kafka

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
)

type KafkaConsumer struct {
	BrokerList string
	Partitions string
	Offset     string
	Verbose    bool
	BufferSize int
	Messages   chan *sarama.ConsumerMessage
	Closing    chan struct{}
	WaitGroup  sync.WaitGroup
}

func (this *KafkaConsumer) ConsumeMessage(topic string, funcs ...interface{}) {

	var kafkaConsumer = KafkaConsumer{
		BrokerList: this.BrokerList,
		Partitions: this.Partitions,
		Offset:     this.Offset,
		Verbose:    this.Verbose,
		BufferSize: this.BufferSize,
		Messages:   make(chan *sarama.ConsumerMessage, defaultBufferSize),
		Closing:    make(chan struct{}),
		WaitGroup:  this.WaitGroup,
	}

	fmt.Println("Start consumeMessage")
	if kafkaConsumer.validate() == false {
		os.Exit(1)
	}

	go kafkaConsumer.waitForKillSignal()

	c, err := sarama.NewConsumer(strings.Split(kafkaConsumer.BrokerList, ","), nil)
	if err != nil {
		fmt.Println("Failed to start consumer:", err)
		os.Exit(1)
	}
	defer kafkaConsumer.closeConsumer(c)

	if kafkaConsumer.getMessageWithGoRoutine(c, topic) == false {
		os.Exit(1)
	}

	go callback(kafkaConsumer.Messages, funcs...)

	kafkaConsumer.WaitGroup.Wait() //program will wait here until receive KILL SIGNAL
	fmt.Println("Done consuming topic", topic)
	close(kafkaConsumer.Messages)
}

func callback(messages chan *sarama.ConsumerMessage, funcs ...interface{}) {
	if len(funcs) == 0 {
		for msg := range messages {
			fmt.Printf("consumed: %s\n", string(msg.Value))
		}
	} else {
		var wg sync.WaitGroup
		for msg := range messages {

			wg.Add(1)

			go func() {
				defer wg.Done()
				fmt.Printf("consumed with callback: %s\n", string(msg.Value))
				execFunction(string(msg.Value), funcs...)
			}()
		}

		wg.Wait()
	}
}

func execFunction(msg string, funcs ...interface{}) (result []reflect.Value, err error) {
	funcs = append(funcs, msg)
	f := reflect.ValueOf(funcs[0])
	params := funcs[1:]
	fmt.Printf("All params(%d): %#v, %#v\n", len(params), params)

	numbOfReceivedParams := len(params)
	numOfFuncInputParams := f.Type().NumIn()
	if numbOfReceivedParams != numOfFuncInputParams {
		fmt.Println("\033[0;31mError: The number of params is not adapted.\033[0m")
		err = errors.New("The number of params is not adapted.")
		return nil, err
	}

	in := make([]reflect.Value, numbOfReceivedParams)
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}
	result = f.Call(in)
	return result, nil
}

func (this *KafkaConsumer) closeConsumer(c sarama.Consumer) {
	err := c.Close()
	if err != nil {
		fmt.Println("Failed to close Kafkaconsumer:", err)
	}
}

func (this *KafkaConsumer) validate() bool {
	if this.BrokerList == "" {
		fmt.Println("You have to provide -brokers as a comma-separated list, or set the KafkaConsumer_PEERS environment variable.")
		return false
	}

	return true
}

func (this *KafkaConsumer) getMessageWithGoRoutine(c sarama.Consumer, topic string) bool {
	var initialOffset int64
	switch this.Offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		fmt.Println("-offset should be `oldest` or `newest`")
		return false
	}

	partitionList, err := this.getPartitions(c, topic)
	if err != nil {
		fmt.Println("Failed to get the list of partitions:", err)
		return false
	}

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(topic, partition, initialOffset)
		if err != nil {
			fmt.Printf("Failed to start Kafkaconsumer for partition %d: %s\n", partition, err)
			return false
		}

		go this.asynClosePartition(pc)

		this.WaitGroup.Add(1)
		go this.getMessagesFromPartition(pc)
	}

	return true
}

func (this *KafkaConsumer) asynClosePartition(pc sarama.PartitionConsumer) {
	<-this.Closing
	pc.AsyncClose()
}

func (this *KafkaConsumer) getMessagesFromPartition(pc sarama.PartitionConsumer) {
	defer this.WaitGroup.Done()
	for message := range pc.Messages() {
		this.Messages <- message
	}
}

func (this *KafkaConsumer) waitForKillSignal() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill, os.Interrupt)
	<-signals
	fmt.Println("Initiating shutdown of Kafkaconsumer...")
	close(this.Closing)
}

func (this *KafkaConsumer) sleepReportSuccess() {
	for msg := range this.Messages {
		fmt.Printf("Partition=%d\tOffset=%d\tKey=%s\tValue=%s\n", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	}
}

func (this *KafkaConsumer) getPartitions(c sarama.Consumer, topic string) ([]int32, error) {
	if this.Partitions == "all" {
		return c.Partitions(topic)
	}

	tmp := strings.Split(this.Partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}
