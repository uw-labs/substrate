package helper

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uw-labs/substrate"
	"golang.org/x/sync/errgroup"
)

func TestAckOrderingSink(t *testing.T) {
	assert := assert.New(t)

	mockSink := &mockSink{}

	sink := NewAckOrderingSink(mockSink)

	ctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(ctx)

	msgs := make(chan substrate.Message)
	acks := make(chan substrate.Message)

	eg.Go(func() error {
		return sink.PublishMessages(ctx, acks, msgs)
	})

	var messages []*myMessage
	for i := 0; i < 15; i++ {
		messages = append(messages, &myMessage{byte(i)})
	}

	for _, m := range messages {
		msgs <- m
	}

	for _, m := range messages {
		ack := <-acks
		assert.Equal(ack, m)
	}

	cancel()

	err := eg.Wait()
	assert.NoError(err)
}

type myMessage struct {
	num byte
}

func (m *myMessage) Data() []byte {
	return []byte{m.num}
}

type mockSink struct{}

func (s *mockSink) Close() error {
	return nil
}

func (s *mockSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) error {
	t := time.NewTicker(100 * time.Millisecond)

	var toAck []substrate.Message

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m := <-messages:
			toAck = append(toAck, m)
		case <-t.C:
			rand.Shuffle(len(toAck), func(i, j int) { toAck[i], toAck[j] = toAck[j], toAck[i] })
			for _, a := range toAck {
				acks <- a
			}
			toAck = toAck[0:0]
		}
	}
}

func (s *mockSink) Status() (*substrate.Status, error) {
	return &substrate.Status{Working: true}, nil
}
