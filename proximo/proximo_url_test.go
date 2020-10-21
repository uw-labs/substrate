package proximo

import (
	"testing"
	"time"

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
			name:  "with-keep-alive",
			input: "proximo://localhost:123/t1?keep-alive-time=60m",
			expected: AsyncMessageSinkConfig{
				Broker: "localhost:123",
				Topic:  "t1",
				KeepAlive: &KeepAlive{
					Time:    time.Minute * 60,
					Timeout: time.Second * 10,
				},
			},
		},
		{
			name:  "with-keep-alive-timeout",
			input: "proximo://localhost:123/t1?keep-alive-time=60m&keep-alive-timeout=70s",
			expected: AsyncMessageSinkConfig{
				Broker: "localhost:123",
				Topic:  "t1",
				KeepAlive: &KeepAlive{
					Time:    time.Minute * 60,
					Timeout: time.Second * 70,
				},
			},
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
		{
			name:  "withdebug",
			input: "proximo://localhost:123/t1?debug=true",
			expected: AsyncMessageSinkConfig{
				Broker: "localhost:123",
				Topic:  "t1",
				Debug:  true,
			},
			expectedErr: nil,
		},
		{
			name:  "with-full-userinfo",
			input: "proximo://test:dummypassword@localhost:123/t1",
			expected: AsyncMessageSinkConfig{
				Broker: "localhost:123",
				Topic:  "t1",
				Credentials: Credentials{
					ClientID: "test",
					Secret:   "dummypassword",
				},
			},
			expectedErr: nil,
		},
		{
			name:  "with-only-user",
			input: "proximo://test@localhost:123/t1",
			expected: AsyncMessageSinkConfig{
				Broker: "localhost:123",
				Topic:  "t1",
				Credentials: Credentials{
					ClientID: "test",
					Secret:   "",
				},
			},
			expectedErr: nil,
		},
		{
			name:  "with-only-password",
			input: "proximo://:dummypassword@localhost:123/t1",
			expected: AsyncMessageSinkConfig{
				Broker: "localhost:123",
				Topic:  "t1",
				Credentials: Credentials{
					ClientID: "",
					Secret:   "dummypassword",
				},
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
			name:  "with-keep-alive",
			input: "proximo://localhost:123/t1?keep-alive-time=60m",
			expected: AsyncMessageSourceConfig{
				Broker: "localhost:123",
				Topic:  "t1",
				KeepAlive: &KeepAlive{
					Time:    time.Minute * 60,
					Timeout: time.Second * 10,
				},
			},
		},
		{
			name:  "with-keep-alive-timeout",
			input: "proximo://localhost:123/t1?keep-alive-time=60m&keep-alive-timeout=70s",
			expected: AsyncMessageSourceConfig{
				Broker: "localhost:123",
				Topic:  "t1",
				KeepAlive: &KeepAlive{
					Time:    time.Minute * 60,
					Timeout: time.Second * 70,
				},
			},
		},
		{
			name:  "with-full-userinfo",
			input: "proximo://test:dummypassword@localhost",
			expected: AsyncMessageSourceConfig{
				Broker: "localhost",
				Credentials: Credentials{
					ClientID: "test",
					Secret:   "dummypassword",
				},
			},
			expectedErr: nil,
		},
		{
			name:  "with-only-user",
			input: "proximo://test@localhost",
			expected: AsyncMessageSourceConfig{
				Broker: "localhost",
				Credentials: Credentials{
					ClientID: "test",
					Secret:   "",
				},
			},
			expectedErr: nil,
		},
		{
			name:  "with-only-password",
			input: "proximo://:dummypassword@localhost",
			expected: AsyncMessageSourceConfig{
				Broker: "localhost",
				Credentials: Credentials{
					ClientID: "",
					Secret:   "dummypassword",
				},
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
