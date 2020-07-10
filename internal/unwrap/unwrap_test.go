package unwrap_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/internal/unwrap"
)

func TestUnwrap(t *testing.T) {
	originalMsg := &message{
		data: []byte("data"),
	}
	wrappedMsg := &annotatedMessage{
		annotation: 1,
		original: &annotatedMessage{
			annotation: 2,
			original: &annotatedMessage{
				annotation: 3,
				original:   originalMsg,
			},
		},
	}
	unwrappedMsg := unwrap.Unwrap(wrappedMsg)

	require.Equal(t, originalMsg, unwrappedMsg)
}

type message struct {
	data []byte
}

func (msg *message) Data() []byte {
	return msg.data
}

func (msg message) Key() []byte {
	return nil
}

type annotatedMessage struct {
	annotation int
	original   substrate.Message
}

func (msg *annotatedMessage) Data() []byte {
	return msg.original.Data()
}

func (msg annotatedMessage) Key() []byte {
	return nil
}

func (msg *annotatedMessage) Original() substrate.Message {
	return msg.original
}
