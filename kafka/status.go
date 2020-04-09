package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/uw-labs/substrate"
)

func status(client sarama.Client, topic string) (*substrate.Status, error) {
	status := &substrate.Status{}

	writablePartitions, err := client.WritablePartitions(topic)
	if err != nil {
		status.Working = false
		status.Problems = append(status.Problems, err.Error())
		return status, nil
	}
	if len(writablePartitions) == 0 {
		status.Working = false
		status.Problems = append(status.Problems, "no writable partitions")
		return status, nil
	}

	status.Working = true
	return status, nil
}
