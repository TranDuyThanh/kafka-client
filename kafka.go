package kafka

const defaultBufferSize = 256

type Kafka struct {
	Producer      KafkaProducer
	Consumer      KafkaConsumer
	ConsumerGroup KafkaConsumerGroup
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
	}

	return &kafka
}
