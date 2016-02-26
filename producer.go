package kafka

import (
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
)

type KafkaProducer struct {
	BrokerList  string
	Key         string
	Partitioner string
	Partition   int
	Verbose     bool
	Silent      bool
}

func (this *KafkaProducer) ProduceMessage(topic string, value string) bool {
	if this.BrokerList == "" {
		fmt.Println("no -brokers specified. Alternatively, set the KAFKA_PEERS environment variable")
		return false
	}

	if topic == "" {
		fmt.Println("no -topic specified")
		return false
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll

	switch this.Partitioner {
	case "":
		if this.Partition >= 0 {
			config.Producer.Partitioner = sarama.NewManualPartitioner
		} else {
			config.Producer.Partitioner = sarama.NewHashPartitioner
		}
	case "hash":
		config.Producer.Partitioner = sarama.NewHashPartitioner
	case "random":
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	case "manual":
		config.Producer.Partitioner = sarama.NewManualPartitioner
		if this.Partition == -1 {
			fmt.Println("-partition is required when partitioning manually")
			return false
		}
	default:
		fmt.Printf("Partitioner %s not supported.\n", this.Partitioner)
		return false
	}

	message := &sarama.ProducerMessage{Topic: topic, Partition: int32(this.Partition)}

	if this.Key != "" {
		message.Key = sarama.StringEncoder(this.Key)
	}

	if value != "" {
		message.Value = sarama.StringEncoder(value)
	} else {
		fmt.Println("-value is required, or you have to provide the value on stdin")
		return false
	}

	producer, err := sarama.NewSyncProducer(strings.Split(this.BrokerList, ","), config)
	if err != nil {
		fmt.Println("Failed to open Kafka producer:", err)
		return false
	}
	defer func() {
		if err := producer.Close(); err != nil {
			fmt.Println("Failed to close Kafka producer cleanly:", err)
		}
	}()

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		fmt.Println("Failed to produce message:", err)
		return false
	} else if !this.Silent {
		fmt.Printf("produced: topic=%s\tpartition=%d\toffset=%d\tmsg=%v\n", topic, partition, offset, message.Value)
	}

	return true
}
