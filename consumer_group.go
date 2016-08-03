package kafka

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"gopkg.in/bsm/sarama-cluster.v2"
)

type KafkaConsumerGroup struct {
	GroupID    string
	Partitions string
	BrokerList string
	Offset     string
	Verbose    bool
	BufferSize int
	Messages   chan *sarama.ConsumerMessage
	Closing    chan struct{}
	WaitGroup  sync.WaitGroup
	Version    sarama.KafkaVersion
}

func (this *KafkaConsumerGroup) ConsumeMessage(group, topic string, funcs ...interface{}) {
	kafkaConsumerGroup := KafkaConsumerGroup{
		GroupID:    group,
		Partitions: this.Partitions,
		BrokerList: this.BrokerList,
		Offset:     "newest",
		Verbose:    false,
		BufferSize: this.BufferSize,
		Messages:   make(chan *sarama.ConsumerMessage, defaultBufferSize),
		Closing:    make(chan struct{}),
		WaitGroup:  this.WaitGroup,
	}
	// Init config
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Version = this.Version

	// Init consumer, consume errors & messages
	clusterConsumer, err := cluster.NewConsumer(
		strings.Split(kafkaConsumerGroup.BrokerList, ","),
		kafkaConsumerGroup.GroupID,
		strings.Split(topic, ","),
		config,
	)

	if err != nil {
		kafkaConsumerGroup.printErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	switch kafkaConsumerGroup.Offset {
	case "oldest":
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		kafkaConsumerGroup.printErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	go func() {
		for err := range clusterConsumer.Errors() {
			fmt.Printf("Error: %s\n", err.Error())
		}
	}()

	go func() {
		for note := range clusterConsumer.Notifications() {
			fmt.Printf("Rebalanced: %+v\n", note)
		}
	}()

	go func() {
		consumerGroupCallback(clusterConsumer, funcs...)
	}()

	kafkaConsumerGroup.waitForKillSignal()

	if err := clusterConsumer.Close(); err != nil {
		fmt.Println("Failed to close consumer: ", err)
	}
}

func (*KafkaConsumerGroup) printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func (*KafkaConsumerGroup) printUsageErrorAndExit(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	os.Exit(64)
}

func (this *KafkaConsumerGroup) waitForKillSignal() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, terminatedSignals...)
	<-signals
	fmt.Println("Initiating shutdown of KafkaconsumerGroup...")
	close(this.Closing)
}
