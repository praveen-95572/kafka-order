package producer

import (
	"encoding/json"
	"fmt"
	"log"

	"order-kafka/model"
	"order-kafka/topic"

	"github.com/IBM/sarama"
)

type Producer struct {
	producer sarama.SyncProducer
}

func NewProducer(brokers []string, config *sarama.Config) (*Producer, error) {
	p, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	return &Producer{producer: p}, nil
}

func (p *Producer) Publish(order model.Order) error {
	bytes, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("failed to marshal order: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic.OrderEvent,
		Key:   sarama.StringEncoder(order.OrderID), // KEY = OrderID
		Value: sarama.ByteEncoder(bytes),
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return err
	}

	log.Printf("Message sent to partition %d at offset %d orderId %s with status %s \n", partition, offset, order.OrderID, order.Status)
	return nil
}
