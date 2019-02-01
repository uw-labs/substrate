package substrate

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"
)

var (
	// ErrSinkAlreadyClosed is an error returned when user tries to publish a message or
	// close a sink after it was already closed.
	ErrSinkAlreadyClosed = errors.New("sink was closed already")
	// ErrSinkClosedDuringSend is an error returned when a sing is closed while sending a message.
	ErrSinkClosedDuringSend = errors.New("sink was closed while sending the message")
)

// NewSynchronousMessageSink returns a new synchronous message sink, given an
// AsyncMessageSink.  When Close is called on the SynchronousMessageSink, this
// is also propogated to the underlying SynchronousMessageSink
func NewSynchronousMessageSink(ams AsyncMessageSink) SynchronousMessageSink {
	spa := &synchronousMessageSinkAdapter{
		ams,

		make(chan struct{}),
		make(chan struct{}),
		nil,

		make(chan *produceReq),
	}
	go spa.loop()
	return spa
}

type synchronousMessageSinkAdapter struct {
	aprod AsyncMessageSink

	closeReq chan struct{}
	closed   chan struct{}
	closeErr error

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
		return spa.aprod.PublishMessages(ctx, acks, toSend)
	})

	eg.Go(func() error {
		var needAcks []*produceReq
		defer func() {
			// Send error to all waiting publish requests before shutting down
			for _, req := range needAcks {
				select {
				case <-req.ctx.Done():
				case req.done <- ErrSinkClosedDuringSend:
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return nil
			case pr := <-spa.toProduce:
				select {
				case <-ctx.Done():
					return nil
				case toSend <- pr.m:
					needAcks = append(needAcks, pr)
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

	spa.closeErr = eg.Wait()
	if spa.closeErr == errSinkClosed {
		spa.closeErr = nil
	}
	close(spa.closed)
}

func (spa *synchronousMessageSinkAdapter) Close() error {
	select {
	case <-spa.closed:
		return ErrSinkAlreadyClosed
	case spa.closeReq <- struct{}{}:
		<-spa.closed
		aCloseErr := spa.aprod.Close()
		if spa.closeErr != nil {
			return spa.closeErr
		}
		return aCloseErr
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
