package main

import (
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"order-kafka/config"
	"order-kafka/model"
	"order-kafka/producer"
	"order-kafka/topic"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	// Kafka brokers
	brokers := []string{"localhost:9092", "localhost:9094", "localhost:9096"}
	cfg := config.NewKafkaConfig()

	// Create topic
	topicName := topic.OrderEvent
	if err := topic.CreateTopic(brokers, topicName, 3, 1); err != nil {
		log.Fatal(err)
	}
	log.Printf("[INFO] Topic '%s' ready", topicName)

	// Initialize producer
	prod, err := producer.NewProducer(brokers, cfg)
	if err != nil {
		log.Fatal("failed to create producer: ", err)
	}
	log.Println("[INFO] Producer ready")

	// Status sequence
	statusSeq := []model.OrderStatus{model.Created, model.Verified, model.Settled}

	// Map to track next status per order ID
	orderStatusMap := make(map[string]int)
	var mu sync.Mutex

	for {
		var wg sync.WaitGroup
		numOrders := 5 // number of orders per batch

		for i := 0; i < numOrders; i++ {
			time.Sleep(1 * time.Second)
			wg.Add(1)
			go func() {
				defer wg.Done()

				// Random order ID between 1000–1005
				orderID := "ORD-" + strconv.Itoa(1000+rand.Intn(6))

				// Determine next status in sequence
				mu.Lock()
				idx := orderStatusMap[orderID]
				status := statusSeq[idx]
				orderStatusMap[orderID] = (idx + 1) % len(statusSeq)
				mu.Unlock()

				order := model.Order{
					OrderID: orderID,
					Status:  status,
				}

				// Publish order
				if err := prod.Publish(order); err != nil {
					log.Printf("[ERROR] Failed to publish order %s: %v", order.OrderID, err)
				} else {
					log.Printf("[INFO] Published order %s with status '%s'", order.OrderID, order.Status)
				}
			}()
		}

		wg.Wait()
		log.Println("[INFO] Batch of orders published")

		// Wait before generating next batch
		time.Sleep(10 * time.Second)
	}
}
