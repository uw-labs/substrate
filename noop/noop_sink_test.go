package noop_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/noop"
)

type Message struct {
	data []byte
}

func (m Message) Data() []byte {
	return m.data
}

func (m Message) Key() []byte {
	return nil
}

func TestPublishMessages(t *testing.T) {
	sink := noop.NewAsyncMessageSink()
	acks := make(chan substrate.Message)
	messages := make(chan substrate.Message)

	sinkContext, sinkCancel := context.WithCancel(context.Background())
	defer sinkCancel()

	errs := make(chan error)

	go func() {
		defer close(errs)
		errs <- sink.PublishMessages(sinkContext, acks, messages)
	}()

	messages <- Message{}

	select {
	case err := <-errs:
		assert.NoError(t, err)
		return
	case <-acks:
		sinkCancel()
	}
}
