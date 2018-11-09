package kafka

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/suburl"
)

func TestKafkaSink(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		name        string
		input       string
		expected    AsyncMessageSinkConfig
		expectedErr error
	}{
		{
			name:  "simple",
			input: "kafka://localhost",
			expected: AsyncMessageSinkConfig{
				Brokers: []string{"localhost"},
			},
			expectedErr: nil,
		},
		{
			name:  "standard",
			input: "kafka://localhost:123/t1",
			expected: AsyncMessageSinkConfig{
				Brokers: []string{"localhost:123"},
				Topic:   "t1",
			},
			expectedErr: nil,
		},
		{
			name:  "with-port",
			input: "kafka://localhost:123/t1",
			expected: AsyncMessageSinkConfig{
				Brokers: []string{"localhost:123"},
				Topic:   "t1",
			},
			expectedErr: nil,
		},
		{
			name:  "everything",
			input: "kafka://localhost:123/t1/?broker=localhost:234&broker=localhost:345",
			expected: AsyncMessageSinkConfig{
				Brokers: []string{"localhost:123", "localhost:234", "localhost:345"},
				Topic:   "t1",
			},
			expectedErr: nil,
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {

			var conf AsyncMessageSinkConfig
			kafkaSinker = func(c AsyncMessageSinkConfig) (substrate.AsyncMessageSink, error) {
				conf = c
				return nil, nil
			}
			_, err := suburl.NewSink(tst.input)

			if tst.expectedErr != err {
				t.Errorf("expected error %v but got %v", tst.expectedErr, err)
			}

			assert.Equal(tst.expected, conf)
		})
	}

}

func TestKafkaSource(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		name        string
		input       string
		expected    AsyncMessageSourceConfig
		expectedErr error
	}{
		{
			name:  "simple",
			input: "kafka://localhost",
			expected: AsyncMessageSourceConfig{
				Brokers: []string{"localhost"},
			},
			expectedErr: nil,
		},
		{
			name:  "standard",
			input: "kafka://localhost:123/t1",
			expected: AsyncMessageSourceConfig{
				Brokers: []string{"localhost:123"},
				Topic:   "t1",
			},
			expectedErr: nil,
		},
		{
			name:  "with-port",
			input: "kafka://localhost:123/t1",
			expected: AsyncMessageSourceConfig{
				Brokers: []string{"localhost:123"},
				Topic:   "t1",
			},
			expectedErr: nil,
		},
		{
			name:  "everything",
			input: "kafka://localhost:123/t1/?offset=latest&consumer-group=g1&metadata-refresh=2s&broker=localhost:234&broker=localhost:345",
			expected: AsyncMessageSourceConfig{
				Brokers:                  []string{"localhost:123", "localhost:234", "localhost:345"},
				ConsumerGroup:            "g1",
				MetadataRefreshFrequency: 2 * time.Second,
				Offset:                   sarama.OffsetNewest,
				Topic:                    "t1",
			},
			expectedErr: nil,
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {

			var conf AsyncMessageSourceConfig
			kafkaSourcer = func(c AsyncMessageSourceConfig) (substrate.AsyncMessageSource, error) {
				conf = c
				return nil, nil
			}
			_, err := suburl.NewSource(tst.input)

			if tst.expectedErr != err {
				t.Errorf("expected error %v but got %v", tst.expectedErr, err)
			}

			assert.Equal(tst.expected, conf)
		})
	}

}
