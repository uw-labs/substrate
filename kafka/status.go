package kafka

import (
	"github.com/IBM/sarama"
	"github.com/uw-labs/substrate"
)

func status(client sarama.Client, topic string) (*substrate.Status, error) {
	status := &substrate.Status{}

	err := client.RefreshMetadata(topic)
	if err != nil {
		status.Working = false
		status.Problems = append(status.Problems, err.Error())
		return status, nil
	}

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

	partitions, err := client.Partitions(topic)
	if err != nil {
		status.Working = false
		status.Problems = append(status.Problems, err.Error())
		return status, nil
	}
	if len(writablePartitions) < len(partitions) {
		status.Working = true
		status.Problems = append(status.Problems, "some partitions are leaderless")
		return status, nil
	}

	status.Working = true
	return status, nil
}
