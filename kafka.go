package kafka

import (
	"github.com/Shopify/sarama"
)

const defaultBufferSize = 256

type Kafka struct {
	Producer KafkaProducer
	Consumer KafkaConsumer
}

func Init(brokerList string) *Kafka {
	var kafka Kafka
	kafka.Producer = KafkaProducer{
		BrokerList:  brokerList,
		Key:         "",
		Partitioner: "random",
		Partition:   -1,
		Verbose:     false,
		Silent:      false,
	}

	kafka.Consumer = KafkaConsumer{
		BrokerList: brokerList,
		Verbose:    false,
		Offset:     "newest",
		Partitions: "all",
		BufferSize: 256, //default
		Messages:   make(chan *sarama.ConsumerMessage, defaultBufferSize),
		Closing:    make(chan struct{}),
	}

	return &kafka
}
