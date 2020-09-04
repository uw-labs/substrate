package liftbridge

import (
	"context"

	lift "github.com/liftbridge-io/go-liftbridge"
	"github.com/uw-labs/substrate"
)

type AsyncMessageSinkConfig struct {
	Brokers []string
	Topic   string
}

func NewAsyncMessageSink(cfg AsyncMessageSinkConfig) (substrate.AsyncMessageSink, error) {
	client, err := lift.Connect(cfg.Brokers)
	if err != nil {
		return nil, err
	}

	return &asyncMessageSink{
		client: client,
		topic:  cfg.Topic,
	}, nil
}

type asyncMessageSink struct {
	client lift.Client
	topic  string
}

func (ams *asyncMessageSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ackErr := make(chan error, 1)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-ackErr:
			return err
		case msg := <-messages:
			if err := ams.client.PublishAsync(ctx, ams.topic, msg.Data(), func(ack *lift.Ack, err error) {
				if err != nil {
					select {
					case <-ctx.Done():
					case ackErr <- err:
					}
					return
				}
				select {
				case <-ctx.Done():
				case acks <- msg:
				}
			}, lift.AckPolicyAll()); err != nil {
				return err
			}
		}
	}

}

func (ams *asyncMessageSink) Close() error {
	return ams.client.Close()
}

func (ams *asyncMessageSink) Status() (*substrate.Status, error) {
	return &substrate.Status{Working: true}, nil
}
