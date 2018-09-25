package substrate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type message []byte

func (m *message) Data() []byte {
	return []byte(*m)
}

func TestSyncConsumeAdapterBasic(t *testing.T) {
	assert := assert.New(t)

	mc := &mockAsyncSource{make(chan Message, 256), make(chan Message, 256), make(chan struct{})}

	m1, m2, m3 := &message{}, &message{}, &message{}
	mc.toSend <- m1
	mc.toSend <- m2
	mc.toSend <- m3

	sc := NewSynchronousMessageSource(mc)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var rcvd []Message
	cb := func(m Message) error {
		rcvd = append(rcvd, m)
		return nil
	}

	if err := sc.ConsumeMessages(ctx, cb); err != nil {
		if err != context.DeadlineExceeded {
			t.Error(err)
		}
	}

	assert.Equal([]Message{
		m1,
		m2,
		m3,
	}, rcvd)

	close(mc.acked)
	var acked []Message
	for ack := range mc.acked {
		acked = append(acked, ack)
	}

	assert.Equal([]Message{
		m1,
		m2,
		m3,
	}, acked)

	sc.Close()
	select {
	case <-mc.closed:
	default:
		t.Error("underlying async source didn't get closed")
	}

}

func TestSyncConsumeAdapterNoAckAfterError(t *testing.T) {
	assert := assert.New(t)

	mc := &mockAsyncSource{make(chan Message, 256), make(chan Message, 256), make(chan struct{})}

	m1, m2, m3 := &message{}, &message{}, &message{}
	mc.toSend <- m1
	mc.toSend <- m2
	mc.toSend <- m3

	sc := NewSynchronousMessageSource(mc)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	errOn3rd := errors.New("error on 3rd message")

	cb := func(m Message) error {
		if m.(*message) == m3 {
			return errOn3rd
		}
		return nil
	}

	assert.Error(sc.ConsumeMessages(ctx, cb), "error on 3rd message")

	close(mc.acked)
	var acked []Message
	for ack := range mc.acked {
		acked = append(acked, ack)
	}

	assert.Equal([]Message{
		m1,
		m2,
	}, acked)

	sc.Close()
	select {
	case <-mc.closed:
	default:
		t.Error("underlying async source didn't get closed")
	}
}

type mockAsyncSource struct {
	toSend chan Message
	acked  chan Message

	closed chan struct{}
}

func (m *mockAsyncSource) ConsumeMessages(ctx context.Context, messages chan<- Message, acks <-chan Message) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-m.toSend:
			select {
			case messages <- msg:
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ack := <-acks:
					m.acked <- ack
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (mock *mockAsyncSource) Close() error {
	close(mock.closed)
	return nil
}

func (m *mockAsyncSource) Status() (*Status, error) {
	return &Status{Working: true}, nil
}
