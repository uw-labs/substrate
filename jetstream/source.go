package jetstream

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/uw-labs/substrate"
)

type Offset int64

const (
	// OffsetOldest indicates the oldest appropriate message available on the broker.
	OffsetOldest Offset = -2
	// OffsetNewest indicates the next appropriate message available on the broker.
	OffsetNewest Offset = -1
)

type AsyncMessageSourceConfig struct {
	URL           string
	Topic         string
	ConsumerGroup string
	Offset        Offset
	AckWait       time.Duration
}

func NewAsyncMessageSource(cfg AsyncMessageSourceConfig) (substrate.AsyncMessageSource, error) {
	conn, err := nats.Connect(cfg.URL)
	if err != nil {
		return nil, err
	}
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, err
	}
	switch _, err := js.ConsumerInfo(cfg.Topic, cfg.ConsumerGroup); {
	case err == nil:
	case err.Error() == "nats: consumer not found":
		// Need to create the consumer manually as when it's created automatically
		// it gets deleted when Drain is called.
		deliverPolicy := nats.DeliverLastPolicy
		if cfg.Offset == OffsetOldest {
			deliverPolicy = nats.DeliverAllPolicy
		}
		if cfg.AckWait == 0 {
			cfg.AckWait = time.Second * 30 // NATS default
		}
		_, err := js.AddConsumer(cfg.Topic, &nats.ConsumerConfig{
			Durable:        cfg.ConsumerGroup,
			DeliverPolicy:  deliverPolicy,
			DeliverSubject: nats.NewInbox(),
			AckPolicy:      nats.AckExplicitPolicy,
			AckWait:        cfg.AckWait,
		})
		if err != nil {
			return nil, err
		}
	default:
		conn.Close()
		return nil, err
	}

	return asyncSourceSource{
		cfg:   cfg,
		conn:  conn,
		jsCtx: js,
	}, nil
}

type asyncSourceSource struct {
	cfg   AsyncMessageSourceConfig
	conn  *nats.Conn
	jsCtx nats.JetStreamContext
}

func (a asyncSourceSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	toAck := make(chan *message)
	handler := func(msg *nats.Msg) {
		sMsg := &message{natsMsg: msg}
		select {
		case <-ctx.Done():
			return
		case toAck <- sMsg:
		}
		select {
		case <-ctx.Done():
			return
		case messages <- sMsg:
		}
	}

	sub, err := a.jsCtx.QueueSubscribe(a.cfg.Topic, a.cfg.ConsumerGroup, handler, nats.Durable(a.cfg.ConsumerGroup), nats.ManualAck())
	if err != nil {
		return err
	}
	defer sub.Drain()

	expectedAcks := make([]*message, 0)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-toAck:
			expectedAcks = append(expectedAcks, msg)
		case ack := <-acks:
			if len(expectedAcks) == 0 {
				return substrate.InvalidAckError{Acked: ack, Expected: nil}
			}
			msgToAck := expectedAcks[0]
			msg, ok := ack.(*message)
			if !ok || msg != msgToAck {
				return substrate.InvalidAckError{Acked: ack, Expected: msgToAck}
			}
			if err := msgToAck.natsMsg.Ack(); err != nil {
				return fmt.Errorf("failed to ack message: %w", err)
			}
			expectedAcks = expectedAcks[1:]
		}
	}
}

func (a asyncSourceSource) Close() error {
	return a.conn.Drain()
}

func (a asyncSourceSource) Status() (*substrate.Status, error) {
	return natsStatus(a.conn)
}

type message struct {
	natsMsg *nats.Msg
}

func (msg *message) Data() []byte {
	return msg.natsMsg.Data
}

func (msg *message) DiscardPayload() {
	msg.natsMsg.Data = nil
}
