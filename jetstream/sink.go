package jetstream

import (
	"context"

	"github.com/nats-io/nats.go"

	"github.com/uw-labs/substrate"
)

const DefaultSinkMaxPending = 256

type AsyncMessageSinkConfig struct {
	URL        string
	Topic      string
	MaxPending int
}

func NewAsyncMessageSink(cfg AsyncMessageSinkConfig) (substrate.AsyncMessageSink, error) {
	conn, err := nats.Connect(cfg.URL)
	if err != nil {
		return nil, err
	}
	if cfg.MaxPending == 0 {
		cfg.MaxPending = DefaultSinkMaxPending
	}
	js, err := conn.JetStream(nats.PublishAsyncMaxPending(cfg.MaxPending))
	if err != nil {
		conn.Close()

		return nil, err
	}
	return asyncSourceSink{
		cfg:   cfg,
		conn:  conn,
		jsCtx: js,
	}, nil
}

type asyncSourceSink struct {
	cfg   AsyncMessageSinkConfig
	conn  *nats.Conn
	jsCtx nats.JetStreamContext
}

func (a asyncSourceSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) error {
	errors := make(chan error, a.cfg.MaxPending)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-messages:
			ack, err := a.jsCtx.PublishAsync(a.cfg.Topic, msg.Data())
			if err != nil {
				return err
			}
			go func() {
				select {
				case <-ack.Ok():
					select {
					case <-ctx.Done():
					case acks <- msg:
					}
				case err := <-ack.Err():
					select {
					case <-ctx.Done():
					case errors <- err:
					}
				}
			}()
		case err := <-errors:
			return err
		}
	}
}

func (a asyncSourceSink) Close() error {
	a.conn.Close()
	return nil
}

func (a asyncSourceSink) Status() (*substrate.Status, error) {
	return natsStatus(a.conn)
}
