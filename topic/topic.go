package topic

import (
	"log"

	"github.com/IBM/sarama"
)

// CreateTopic creates a Kafka topic if it doesn't exist
func CreateTopic(brokers []string, topicName string, numPartitions int32, replicationFactor int16) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0
	var retentionday = "604800000" // 7 days
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return err
	}
	defer admin.Close()

	// List existing topics
	topics, err := admin.ListTopics()
	if err != nil {
		return err
	}

	// Create main topic if missing
	if _, ok := topics[topicName]; !ok {
		topicDetail := &sarama.TopicDetail{
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
			ConfigEntries: map[string]*string{
				"retention.ms": &retentionday,
			},
		}
		err = admin.CreateTopic(topicName, topicDetail, false)
		if err != nil {
			return err
		}
		log.Printf("Topic %s created successfully", topicName)
	} else {
		log.Printf("Topic %s already exists", topicName)
	}

	// Create DLQ topic
	dlqTopic := topicName + "-dlq"
	if _, ok := topics[dlqTopic]; !ok {
		topicDetail := &sarama.TopicDetail{
			NumPartitions:     1, // single partition for DLQ
			ReplicationFactor: replicationFactor,
			ConfigEntries: map[string]*string{
				"retention.ms": &retentionday,
			},
		}
		err = admin.CreateTopic(dlqTopic, topicDetail, false)
		if err != nil {
			return err
		}
		log.Printf("DLQ Topic %s created successfully", dlqTopic)
	} else {
		log.Printf("DLQ Topic %s already exists", dlqTopic)
	}

	return nil
}
