package sync

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/sync/rungroup"
)

// SynchronousMessageSink is an extension of substrate.SynchronousMessageSink that exposes the Run method
type SynchronousMessageSink interface {
	substrate.SynchronousMessageSink
	Run(ctx context.Context, b backoff.BackOff) error
}

// AsyncMessageSinkFactory is a function that initialises asynchronous message sink
type AsyncMessageSinkFactory func() (substrate.AsyncMessageSink, error)

type synchronousMessageSinkAdapter struct {
	mutex       sync.Mutex
	started     bool
	ctx         context.Context
	sink        substrate.AsyncMessageSink
	sinkFactory AsyncMessageSinkFactory

	closeReq chan struct{}
	closed   chan error

	toProduce chan *produceReq
}

// NewSynchronousMessageSink returns a new synchronous message sink, given an
// AsyncMessageSinkFactory. When Close is called on the SynchronousMessageSink, this
// is also propagated to the underlying AsyncMessageSink.
func NewSynchronousMessageSink(sinkFactory AsyncMessageSinkFactory) SynchronousMessageSink {
	return &synchronousMessageSinkAdapter{
		sinkFactory: sinkFactory,

		closeReq:  make(chan struct{}),
		closed:    make(chan error, 1),
		toProduce: make(chan *produceReq),
	}
}

type produceReq struct {
	m    substrate.Message
	done chan error
	ctx  context.Context
}

// Run starts the asynchronous backend and blocks until it stops.
func (spa *synchronousMessageSinkAdapter) Run(ctx context.Context, b backoff.BackOff) (outErr error) {
	spa.mutex.Lock()
	if spa.started {
		spa.mutex.Unlock()
		return ErrAlreadyStarted
	}

	sink, err := spa.sinkFactory()
	if err != nil {
		spa.mutex.Unlock()
		return err
	}
	spa.ctx = ctx
	spa.sink = sink
	spa.started = true
	spa.mutex.Unlock()

	if b != nil {
		outErr = backoff.RetryNotify(spa.runPipeline, backoff.WithContext(b, ctx), spa.reconnect)
	} else {
		outErr = spa.runPipeline()
	}

	if outErr == context.Canceled {
		outErr = nil
	}
	spa.closed <- spa.sink.Close()
	close(spa.closed)

	return outErr
}

func (spa *synchronousMessageSinkAdapter) reconnect(err error, _ time.Duration) {
	spa.mutex.Lock()
	defer spa.mutex.Unlock()

	log.Printf("syncronous sink failed with: %s", err)
	if err := spa.sink.Close(); err != nil {
		log.Printf("closing the sink failed with: %s", err)
	}
	sink, err := spa.sinkFactory()
	if err != nil {
		spa.sink = nil
		log.Printf("failed to connect to the sink: %s", err)
	}
	spa.sink = sink
}

func (spa *synchronousMessageSinkAdapter) passMessages(ctx context.Context, toSend chan<- substrate.Message, acks <-chan substrate.Message) error {
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
				return nil
			}
		case ack := <-acks:
			if needAcks[0].m != ack {
				panic("bug")
			}
			close(needAcks[0].done)
			needAcks = needAcks[1:]
		case <-spa.closeReq:
			return nil
		}
	}
}

func (spa *synchronousMessageSinkAdapter) runPipeline() error {
	if spa.sink == nil {
		// Return error so we can try connecting again
		return ErrNotConnected
	}

	toSend := make(chan substrate.Message)
	acks := make(chan substrate.Message)

	rg, ctx := rungroup.New(spa.ctx)

	// no need to lock the sink in the following functions as reconnect is only called after this function terminates
	// and this function starts only after Run was called
	rg.Go(func() error {
		return checkBackend(ctx, spa.sink)
	})
	rg.Go(func() error {
		return spa.sink.PublishMessages(ctx, acks, toSend)
	})
	rg.Go(func() error {
		return spa.passMessages(ctx, toSend, acks)
	})

	return rg.Wait()
}

func (spa *synchronousMessageSinkAdapter) PublishMessage(ctx context.Context, m substrate.Message) error {
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

func (spa *synchronousMessageSinkAdapter) Close() error {
	select {
	case err, ok := <-spa.closed:
		if ok {
			return err
		}
		return ErrSinkAlreadyClosed
	case spa.closeReq <- struct{}{}:
		// Check if channel is still open in case Close
		// is called concurrently more than once
		err, ok := <-spa.closed
		if ok {
			return err
		}
		return ErrSinkAlreadyClosed
	}
}

func (spa *synchronousMessageSinkAdapter) Status() (*substrate.Status, error) {
	spa.mutex.Lock()
	defer spa.mutex.Unlock()
	if spa.sink == nil {
		return nil, ErrNotConnected
	}

	return spa.sink.Status()
}
