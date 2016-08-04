package kafka

import "github.com/Shopify/sarama"

const defaultBufferSize = 256

type Kafka struct {
	Producer      KafkaProducer
	Consumer      KafkaConsumer
	ConsumerGroup KafkaConsumerGroup
	Version       sarama.KafkaVersion
}

func getVersion(versionStr string) sarama.KafkaVersion {
	switch versionStr {
	case "0.8.2.0":
		return sarama.V0_8_2_0
	case "0.8.2.1":
		return sarama.V0_8_2_1
	case "0.8.2.2":
		return sarama.V0_8_2_2
	case "0.9.0.0":
		return sarama.V0_9_0_0
	case "0.9.0.1":
		return sarama.V0_9_0_1
	case "0.10.0.0":
		return sarama.V0_10_0_0
	default:
		panic("Version unsupported.")
	}
}

func Init(brokerList string) *Kafka {
	return InitWithBrokerVersion(brokerList, "0.9.0.0")
}

func InitWithBrokerVersion(brokerList string, version string) *Kafka {
	kafka := Kafka{}
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
		Messages:   nil,
		Closing:    nil,
	}

	kafka.ConsumerGroup = KafkaConsumerGroup{
		BrokerList: brokerList,
		Verbose:    false,
		Offset:     "newest",
		Partitions: "all",
		BufferSize: 256, //default
		Messages:   nil,
		Closing:    nil,
		Version:    getVersion(version),
	}

	return &kafka
}
