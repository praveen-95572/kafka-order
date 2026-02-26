package consumer

import (
	"context"
	"log"
	"order-kafka/topic"

	"github.com/IBM/sarama"
)

func StartConsumer(brokers []string, config *sarama.Config) {

	group := "order-group"

	consumerGroup, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		log.Fatal(err)
	}

	handler := &OrderConsumer{}

	for {
		err := consumerGroup.Consume(context.Background(),
			[]string{topic.OrderEvent}, handler)
		if err != nil {
			log.Fatal(err)
		}
	}
}
