package consumer

import (
	"encoding/json"
	"log"

	"order-kafka/model"

	"github.com/IBM/sarama"
)

type OrderConsumer struct{}

func (c *OrderConsumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (c *OrderConsumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (c *OrderConsumer) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {

	for message := range claim.Messages() {

		var order model.Order
		if err := json.Unmarshal(message.Value, &order); err != nil {
			// sendToDLQ(session, message)
			log.Printf("ERROR unmarshaling message at offset %d: %v", message.Offset, err)
			continue // skip bad message
		}

		log.Printf("Received Order: %+v\n", order)

		// Business Logic
		processOrder(order)

		// MANUAL ACK (Offset Commit)
		session.MarkMessage(message, "")
		session.Commit() // commits immediately - Frequent commits can reduce throughput
	}

	return nil
}

func processOrder(order model.Order) {
	log.Println("Processing:", order.OrderID, order.Status)
}
