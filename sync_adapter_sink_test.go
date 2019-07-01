package substrate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSyncProduceAdapterBasic(t *testing.T) {
	assert := assert.New(t)

	ap := &mockAsyncSink{5, make(chan struct{}, 1)}

	sc := NewSynchronousMessageSink(ap)

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
	case <-ap.closed:
	default:
		t.Error("underlying async sink didn't get closed")
	}
	assert.Equal(ErrSinkAlreadyClosed, sc.PublishMessage(ctx, &m5))

	assert.Equal(ErrSinkAlreadyClosed, sc.Close())
	assert.Equal(ErrSinkAlreadyClosed, sc.PublishMessage(ctx, &m1))
}

func TestSyncProduceAdapter_ErrorOnSend(t *testing.T) {
	ap := &mockAsyncSink{0, make(chan struct{}, 1)}
	sc := NewSynchronousMessageSink(ap)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	msg := message([]byte{'t'})

	assert.Equal(t, ErrSinkClosedOrFailedDuringSend, sc.PublishMessage(ctx, &msg))
	assert.Equal(t, errSeenAllMessages, sc.Close())
	assert.Equal(t, ErrSinkAlreadyClosed, sc.Close())
}

var errSeenAllMessages = errors.New("mock sink saw specified number of messages")

type mockAsyncSink struct {
	toAckCount int
	closed     chan struct{}
}

func (mock *mockAsyncSink) PublishMessages(ctx context.Context, acks chan<- Message, messages <-chan Message) error {
	for {

		select {
		case <-ctx.Done():
			return ctx.Err()
		case m := <-messages:
			if mock.toAckCount > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
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

func (mock *mockAsyncSink) Status() (*Status, error) {
	return &Status{Working: true}, nil
}
