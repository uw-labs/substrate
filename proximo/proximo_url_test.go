package proximo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/suburl"
)

func TestProximoSink(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		name        string
		input       string
		expected    AsyncMessageSinkConfig
		expectedErr error
	}{
		{
			name:  "simple",
			input: "proximo://localhost/topic",
			expected: AsyncMessageSinkConfig{
				Broker: "localhost",
				Topic:  "topic",
			},
			expectedErr: nil,
		},
		{
			name:  "with-port",
			input: "proximo://localhost:123/t1",
			expected: AsyncMessageSinkConfig{
				Broker: "localhost:123",
				Topic:  "t1",
			},
			expectedErr: nil,
		},
		{
			name:  "insecure",
			input: "proximo://localhost:123/t1?insecure=true",
			expected: AsyncMessageSinkConfig{
				Broker:   "localhost:123",
				Topic:    "t1",
				Insecure: true,
			},
			expectedErr: nil,
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {

			var conf AsyncMessageSinkConfig
			proximoSinker = func(c AsyncMessageSinkConfig) (substrate.AsyncMessageSink, error) {
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

func TestProximoSource(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		name        string
		input       string
		expected    AsyncMessageSourceConfig
		expectedErr error
	}{
		{
			name:  "simple",
			input: "proximo://localhost",
			expected: AsyncMessageSourceConfig{
				Broker: "localhost",
			},
			expectedErr: nil,
		},
		{
			name:  "with-port",
			input: "proximo://localhost:123/t1",
			expected: AsyncMessageSourceConfig{
				Broker: "localhost:123",
				Topic:  "t1",
			},
			expectedErr: nil,
		},
		{
			name:  "insecure",
			input: "proximo://localhost:123/t1?insecure=true",
			expected: AsyncMessageSourceConfig{
				Broker:   "localhost:123",
				Topic:    "t1",
				Insecure: true,
			},
			expectedErr: nil,
		},
		{
			name:  "everything",
			input: "proximo://localhost:123/t1/?offset=newest&consumer-group=g1",
			expected: AsyncMessageSourceConfig{
				Broker:        "localhost:123",
				ConsumerGroup: "g1",
				Offset:        OffsetNewest,
				Topic:         "t1",
			},
			expectedErr: nil,
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {

			var conf AsyncMessageSourceConfig
			proximoSourcer = func(c AsyncMessageSourceConfig) (substrate.AsyncMessageSource, error) {
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
