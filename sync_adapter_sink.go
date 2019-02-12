package substrate

import (
	"context"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

var (
	// ErrSinkAlreadyClosed is an error returned when user tries to publish a message or
	// close a sink after it was already closed.
	ErrSinkAlreadyClosed = errors.New("sink was closed already")
	// ErrSinkClosedOrFailedDuringSend is an error returned when a sink is closed or fails while sending a message.
	ErrSinkClosedOrFailedDuringSend = errors.New("sink was closed or failed while sending the message")
)

// NewSynchronousMessageSink returns a new synchronous message sink, given an
// AsyncMessageSink.  When Close is called on the SynchronousMessageSink, this
// is also propogated to the underlying SynchronousMessageSink
func NewSynchronousMessageSink(ams AsyncMessageSink) SynchronousMessageSink {
	spa := &synchronousMessageSinkAdapter{
		ams,

		make(chan struct{}),
		make(chan error, 1),
		make(chan *produceReq),
	}
	go spa.loop()
	return spa
}

type synchronousMessageSinkAdapter struct {
	aprod AsyncMessageSink

	closeReq chan struct{}
	closed   chan error

	toProduce chan *produceReq
}

type produceReq struct {
	m    Message
	done chan error
	ctx  context.Context
}

func (spa *synchronousMessageSinkAdapter) loop() {

	toSend := make(chan Message)
	acks := make(chan Message)

	errSinkClosed := errors.New("sink closed")
	eg, ctx := errgroup.WithContext(context.Background())

	eg.Go(func() error {
		if err := spa.aprod.PublishMessages(ctx, acks, toSend); err != nil {
			return err
		}
		return errSinkClosed // return error to shutdown the main loop
	})

	eg.Go(func() error {
		var needAcks []*produceReq
		defer func() {
			// Send error to all waiting publish requests before shutting down
			for _, req := range needAcks {
				select {
				case <-req.ctx.Done():
				case req.done <- ErrSinkClosedOrFailedDuringSend:
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return nil
			case pr := <-spa.toProduce:
				needAcks = append(needAcks, pr)
				select {
				case <-ctx.Done():
					return nil
				case toSend <- pr.m:
				case <-spa.closeReq:
					// Need to return error to stop the publisher
					return errSinkClosed
				}
			case ack := <-acks:
				if needAcks[0].m != ack {
					panic("bug")
				}
				close(needAcks[0].done)
				needAcks = needAcks[1:]
			case <-spa.closeReq:
				// Need to return error to stop the publisher
				return errSinkClosed
			}
		}
	})

	// Wait for sink and loop to terminate and send close error tp closed channel
	if sinkErr := eg.Wait(); sinkErr == nil || sinkErr == errSinkClosed {
		spa.closed <- spa.aprod.Close()
	} else {
		if err := spa.aprod.Close(); err != nil {
			spa.closed <- errors.Errorf("sink error: %v sink close error: %v", sinkErr, err)
		} else {
			spa.closed <- sinkErr
		}
	}
	close(spa.closed)
}

func (spa *synchronousMessageSinkAdapter) Close() error {
	select {
	case err, ok := <-spa.closed:
		if ok {
			return err
		}
		return ErrSinkAlreadyClosed
	case spa.closeReq <- struct{}{}:
		// Check if channel is still open in case Close
		// is called concurrently more than onces
		err, ok := <-spa.closed
		if ok {
			return err
		}
		return ErrSinkAlreadyClosed
	}
}

func (spa *synchronousMessageSinkAdapter) PublishMessage(ctx context.Context, m Message) error {
	pr := &produceReq{m, make(chan error), ctx}

	select {
	case spa.toProduce <- pr:
		select {
		case err := <-pr.done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-spa.closed:
		return ErrSinkAlreadyClosed
	case <-ctx.Done():
		return ctx.Err()
	}

}

func (spa *synchronousMessageSinkAdapter) Status() (*Status, error) {
	return spa.aprod.Status()
}
