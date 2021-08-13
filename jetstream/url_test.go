package jetstream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/suburl"
)

func TestJetStreamURLSink(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		name        string
		input       string
		expected    AsyncMessageSinkConfig
		expectedErr bool
	}{
		{
			name:  "simple",
			input: "jet-stream://localhost/t1",
			expected: AsyncMessageSinkConfig{
				URL:   "http://localhost",
				Topic: "t1",
			},
			expectedErr: false,
		},
		{
			name:  "simple-trailing-slash",
			input: "jet-stream://localhost/t1/",
			expected: AsyncMessageSinkConfig{
				URL:   "http://localhost",
				Topic: "t1",
			},
			expectedErr: false,
		},
		{
			name:  "with-port",
			input: "jet-stream://localhost:123/t1",
			expected: AsyncMessageSinkConfig{
				URL:   "http://localhost:123",
				Topic: "t1",
			},
			expectedErr: false,
		},
		{
			name:        "extra-path-elements",
			input:       "jet-stream://localhost:123/aa/bb",
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

func TestJetStreamURLSource(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		name        string
		input       string
		expected    AsyncMessageSourceConfig
		expectedErr bool
	}{
		{
			name:  "simple",
			input: "jet-stream://localhost/t1",
			expected: AsyncMessageSourceConfig{
				URL:   "http://localhost",
				Topic: "t1",
			},
			expectedErr: false,
		},
		{
			name:  "simple-trailing-slash",
			input: "jet-stream://localhost/t1/",
			expected: AsyncMessageSourceConfig{
				URL:   "http://localhost",
				Topic: "t1",
			},
			expectedErr: false,
		},
		{
			name:  "with-port",
			input: "jet-stream://localhost:123/t1",
			expected: AsyncMessageSourceConfig{
				URL:   "http://localhost:123",
				Topic: "t1",
			},
			expectedErr: false,
		},
		{
			name:  "everything",
			input: "jet-stream://localhost:123/t1?consumer-group=qg1&ack-wait=30s&offset=oldest",
			expected: AsyncMessageSourceConfig{
				URL:           "http://localhost:123",
				ConsumerGroup: "qg1",
				Topic:         "t1",
				AckWait:       30 * time.Second,
				Offset:        OffsetOldest,
			},
			expectedErr: false,
		},
		{
			name:        "extra-path-elements",
			input:       "jet-stream://localhost:123/aa/bb",
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
