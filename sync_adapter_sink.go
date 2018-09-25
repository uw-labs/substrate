package substrate

import (
	"context"
	"errors"
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
	done chan struct{}
	ctx  context.Context
}

func (spa *synchronousMessageSinkAdapter) loop() {

	toSend := make(chan Message)
	acks := make(chan Message)

	ctx, cancel := context.WithCancel(context.Background())
	produceErr := make(chan error)

	go func() {
		produceErr <- spa.aprod.PublishMessages(ctx, acks, toSend)
	}()

	var needAcks []*produceReq

mainLoop:
	for {
		select {
		case pr := <-spa.toProduce:
			select {
			case toSend <- pr.m:
				needAcks = append(needAcks, pr)
			case <-spa.closeReq:
				break mainLoop
			}
		case ack := <-acks:
			if needAcks[0].m != ack {
				panic("bug")
			}
			close(needAcks[0].done)
			needAcks = needAcks[1:]
		case <-spa.closeReq:
			break mainLoop
		}
	}

	cancel()
	spa.closeErr = <-produceErr
	if spa.closeErr == context.Canceled {
		spa.closeErr = nil
	}
	close(spa.closed)
}

func (spa *synchronousMessageSinkAdapter) Close() error {
	select {
	case <-spa.closed:
		return errors.New("SynchronousMessageSinkAdapter is already closed")
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
	pr := &produceReq{m, make(chan struct{}), ctx}

	select {
	case spa.toProduce <- pr:
		select {
		case <-pr.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}

}

func (spa *synchronousMessageSinkAdapter) Status() (*Status, error) {
	return spa.aprod.Status()
}
