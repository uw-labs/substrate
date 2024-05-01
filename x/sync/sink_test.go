package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uw-labs/substrate"
)

type message []byte

func (m *message) Data() []byte {
	return []byte(*m)
}

func TestSyncProduceAdapterBasic(t *testing.T) {
	assert := require.New(t)

	sc := NewSynchronousMessageSink(newMockAsynSinkFactory(5))
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		assert.NoError(sc.Run(ctx, nil))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	m1, m2, m3, m4, m5 := message([]byte{'a'}), message([]byte{'b'}), message([]byte{'c'}), message([]byte{'d'}), message([]byte{'e'})
	assert.NoError(sc.PublishMessage(ctx, &m1))
	assert.NoError(sc.PublishMessage(ctx, &m2))
	assert.NoError(sc.PublishMessage(ctx, &m3))
	assert.NoError(sc.PublishMessage(ctx, &m4))
	assert.NoError(sc.PublishMessage(ctx, &m5))

	assert.NoError(sc.Close())

	select {
	case <-sc.(*synchronousMessageSinkAdapter).closed:
	default:
		t.Error("underlying async sink didn't get closed")
	}

	assert.Equal(ErrSinkAlreadyClosed, sc.Close())
	assert.Equal(ErrSinkAlreadyClosed, sc.PublishMessage(ctx, &m1))
}

func TestSyncProduceAdapter_ErrorOnSend(t *testing.T) {
	assert := require.New(t)
	sc := NewSynchronousMessageSink(newMockAsynSinkFactory(0))

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		assert.Equal(errSeenAllMessages, sc.Run(ctx, nil))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msg := message([]byte{'t'})
	assert.Equal(ErrSinkClosedOrFailedDuringSend, sc.PublishMessage(ctx, &msg))

	assert.Equal(nil, sc.Close())
	assert.Equal(ErrSinkAlreadyClosed, sc.Close())
}

var errSeenAllMessages = errors.New("mock sink saw specified number of messages")

type mockAsyncSink struct {
	toAckCount int
	closed     chan struct{}
}

func newMockAsynSinkFactory(toAckCount int) AsyncMessageSinkFactory {
	return func() (substrate.AsyncMessageSink, error) {
		return &mockAsyncSink{toAckCount, make(chan struct{}, 1)}, nil
	}
}

func (mock *mockAsyncSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) error {
	for {

		select {
		case <-ctx.Done():
			return ctx.Err()
		case m := <-messages:
			if mock.toAckCount > 0 {
				select {
				case <-ctx.Done():
					return nil
				case acks <- m:
				}
				mock.toAckCount--
			} else {
				return errSeenAllMessages
			}
		}
	}
}

func (mock *mockAsyncSink) Close() error {
	close(mock.closed)
	return nil
}

func (mock *mockAsyncSink) Status() (*substrate.Status, error) {
	return &substrate.Status{Working: true}, nil
}
