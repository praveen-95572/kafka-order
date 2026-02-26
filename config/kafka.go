package config

import (
	"github.com/IBM/sarama"
)

func NewKafkaConfig() *sarama.Config {
	config := sarama.NewConfig()

	// Producer reliability
	config.Producer.RequiredAcks = sarama.WaitForAll
	/*
		sarama.NoResponse - fire-and-forget
		sarama.WaitForLocal - only leader must acknowledge
	*/
	config.Producer.Retry.Max = 5     //Number of times the producer will retry sending a failed message.
	config.Producer.Idempotent = true //preventing duplicate messages in case of retries.
	config.Net.MaxOpenRequests = 1
	config.Producer.Return.Successes = true
	//Tells Sarama to return successful messages to your producer channel.
	// Without this, you cannot confirm that a message was delivered.

	// Consumer config
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	/*
		BalanceStrategyRange - assign contiguous partitions to consumers
		BalanceStrategyRoundRobin - assign partitions evenly in round-robin fashion
		BalanceStrategySticky → minimizes partition movement between rebalances
	*/
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	/*
		sarama.OffsetNewest → start at the latest messages (common for streaming IoT data)
		sarama.OffsetOldest → start from beginning of the topic
	*/

	// Manual commit disabled by default
	config.Consumer.Offsets.AutoCommit.Enable = false
	/*
		false → you manually commit offsets, giving you precise control (recommended in critical systems)
		true → offsets auto-committed at intervals (simpler but may lose progress on crashes)
	*/

	return config
}
