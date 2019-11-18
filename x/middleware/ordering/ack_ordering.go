package ordering

import (
	"context"

	"github.com/pkg/errors"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/sync/rungroup"
)

// NewAckOrderingMessageSource is a message source that accepts acknowledgements in any order and
// forwards them to underlying source in the order in which the messages are read.
func NewAckOrderingMessageSource(delegate substrate.AsyncMessageSource) substrate.AsyncMessageSource {
	return &ackOrderingMiddleware{
		delegate: delegate,
	}
}

type ackOrderingMiddleware struct {
	delegate substrate.AsyncMessageSource
}

func (m *ackOrderingMiddleware) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	rg, ctx := rungroup.New(ctx)
	delegateMsgs := make(chan substrate.Message)
	delegateAcks := make(chan substrate.Message)

	rg.Go(func() error {
		return m.delegate.ConsumeMessages(ctx, delegateMsgs, delegateAcks)
	})
	rg.Go(func() error {
		return m.passMessages(ctx, messages, delegateMsgs)
	})
	rg.Go(func() error {
		return m.passAcks(ctx, acks, delegateAcks)
	})

	return rg.Wait()
}

func (m *ackOrderingMiddleware) passMessages(ctx context.Context, messages chan<- substrate.Message, delegateMsgs <-chan substrate.Message) error {
	var seq uint64
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-delegateMsgs:
			select {
			case <-ctx.Done():
			case messages <- &ackMessage{msg: msg, seq: seq}:
				seq++
			}
		}
	}
}

func (m *ackOrderingMiddleware) passAcks(ctx context.Context, acks <-chan substrate.Message, delegateAcks chan<- substrate.Message) error {
	var seq uint64
	toAck := make(map[uint64]substrate.Message)

	for {
		select {
		case <-ctx.Done():
			return nil
		case ack := <-acks:
			msg, ok := ack.(*ackMessage)
			if !ok {
				return errors.Errorf("invalid ack message type: %v", ack)
			}
			toAck[msg.seq] = msg.msg
			dMsg, ok := toAck[seq]

			for ok {
				select {
				case <-ctx.Done():
					return nil
				case delegateAcks <- dMsg:
					delete(toAck, seq)

					seq++
					dMsg, ok = toAck[seq]
				}
			}
		}
	}
}

func (m *ackOrderingMiddleware) Close() error {
	return m.delegate.Close()
}

func (m *ackOrderingMiddleware) Status() (*substrate.Status, error) {
	return m.delegate.Status()
}

type ackMessage struct {
	msg substrate.Message
	seq uint64
}

func (msg *ackMessage) Data() []byte {
	return msg.Data()
}

func (msg *ackMessage) DiscardPayload() {
	if dMsg, ok := msg.msg.(substrate.DiscardableMessage); ok {
		dMsg.DiscardPayload()
	}
}
