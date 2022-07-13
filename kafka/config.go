package kafka

import "github.com/Shopify/sarama"

type SASLMechanism uint8

const (
	SASLMechanismPlain SASLMechanism = iota
)

func (m SASLMechanism) sarama() sarama.SASLMechanism {
	switch m {
	default:
		fallthrough
	case SASLMechanismPlain:
		return sarama.SASLTypePlaintext
	}
}

// SASLConfig provides a mechanism to authenticate with a Kafka cluster
// using SASL.
type SASLConfig struct {
	Mechanism SASLMechanism
	Username  string
	Password  string
	Version   int16
}
