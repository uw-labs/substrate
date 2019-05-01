package natsstreaming

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/suburl"
)

func TestNatsStreamingURLSink(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		name        string
		input       string
		expected    AsyncMessageSinkConfig
		expectedErr bool
	}{
		{
			name:  "simple",
			input: "nats-streaming://localhost/t1",
			expected: AsyncMessageSinkConfig{
				URL:                    "nats://localhost",
				Subject:                "t1",
				ConnectionNumPings:     3,
				ConnectionPingInterval: 1,
			},
			expectedErr: false,
		},
		{
			name:  "simple-trailing-slash",
			input: "nats-streaming://localhost/t1/",
			expected: AsyncMessageSinkConfig{
				URL:                    "nats://localhost",
				Subject:                "t1",
				ConnectionNumPings:     3,
				ConnectionPingInterval: 1,
			},
			expectedErr: false,
		},
		{
			name:  "with-port",
			input: "nats-streaming://localhost:123/t1",
			expected: AsyncMessageSinkConfig{
				URL:                    "nats://localhost:123",
				Subject:                "t1",
				ConnectionNumPings:     3,
				ConnectionPingInterval: 1,
			},
			expectedErr: false,
		},
		{
			name:  "everything",
			input: "nats-streaming://localhost:123/t1?cluster-id=cid-1&client-id=client-1&ping-timeout=10&ping-num-tries=5",
			expected: AsyncMessageSinkConfig{
				URL:                    "nats://localhost:123",
				ClusterID:              "cid-1",
				ClientID:               "client-1",
				Subject:                "t1",
				ConnectionNumPings:     5,
				ConnectionPingInterval: 10,
			},
			expectedErr: false,
		},
		{
			name:        "extra-path-elements",
			input:       "nats-streaming://localhost:123/aa/bb",
			expected:    AsyncMessageSinkConfig{},
			expectedErr: true,
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			var c AsyncMessageSinkConfig
			natsStreamingSinker = func(conf AsyncMessageSinkConfig) (substrate.AsyncMessageSink, error) {
				c = conf
				return nil, nil
			}
			_, err := suburl.NewSink(tst.input)

			if tst.expectedErr == (err == nil) {
				t.Errorf("expected error %v but got %v", tst.expectedErr, err)
			}

			assert.Equal(tst.expected, c)
		})
	}

}

func TestNatsStreamingURLSource(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		name        string
		input       string
		expected    AsyncMessageSourceConfig
		expectedErr bool
	}{
		{
			name:  "simple",
			input: "nats-streaming://localhost/t1",
			expected: AsyncMessageSourceConfig{
				URL:                    "nats://localhost",
				Subject:                "t1",
				ConnectionNumPings:     3,
				ConnectionPingInterval: 1,
			},
			expectedErr: false,
		},
		{
			name:  "simple-trailing-slash",
			input: "nats-streaming://localhost/t1/",
			expected: AsyncMessageSourceConfig{
				URL:                    "nats://localhost",
				Subject:                "t1",
				ConnectionNumPings:     3,
				ConnectionPingInterval: 1,
			},
			expectedErr: false,
		},
		{
			name:  "with-port",
			input: "nats-streaming://localhost:123/t1",
			expected: AsyncMessageSourceConfig{
				URL:                    "nats://localhost:123",
				Subject:                "t1",
				ConnectionNumPings:     3,
				ConnectionPingInterval: 1,
			},
			expectedErr: false,
		},
		{
			name:  "everything",
			input: "nats-streaming://localhost:123/t1?cluster-id=cid1&client-id=clie1&queue-group=qg1&ack-wait=30s&max-in-flight=1234&offset=oldest&ping-timeout=10&ping-num-tries=5",
			expected: AsyncMessageSourceConfig{
				URL:                    "nats://localhost:123",
				ClusterID:              "cid1",
				QueueGroup:             "qg1",
				ClientID:               "clie1",
				Subject:                "t1",
				AckWait:                30 * time.Second,
				MaxInFlight:            1234,
				Offset:                 OffsetOldest,
				ConnectionNumPings:     5,
				ConnectionPingInterval: 10,
			},
			expectedErr: false,
		},
		{
			name:        "extra-path-elements",
			input:       "nats-streaming://localhost:123/aa/bb",
			expected:    AsyncMessageSourceConfig{},
			expectedErr: true,
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			var c AsyncMessageSourceConfig
			natsStreamingSourcer = func(conf AsyncMessageSourceConfig) (substrate.AsyncMessageSource, error) {
				c = conf
				return nil, nil
			}
			_, err := suburl.NewSource(tst.input)

			if tst.expectedErr == (err == nil) {
				t.Errorf("expected error %v but got %v", tst.expectedErr, err)
			}

			assert.Equal(tst.expected, c)
		})
	}

}
