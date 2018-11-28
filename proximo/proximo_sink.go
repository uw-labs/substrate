package proximo

import (
	"context"

	proximoc "github.com/uw-labs/proximo/proximoc-go"
	"github.com/uw-labs/substrate"
)

var (
	_ substrate.AsyncMessageSink = (*asyncMessageSink)(nil)
)

type AsyncMessageSinkConfig struct {
	Broker   string
	Topic    string
	Insecure bool
}

func NewAsyncMessageSink(config AsyncMessageSinkConfig) (substrate.AsyncMessageSink, error) {

	client, err := proximoc.DialProducer(
		context.Background(),
		config.Broker,
		config.Topic,
	)
	if err != nil {
		return nil, err
	}

	sink := asyncMessageSink{
		client: client,
		Topic:  config.Topic,
	}
	return &sink, nil
}

type asyncMessageSink struct {
	client *proximoc.ProducerConn
	Topic  string
}

func (ams *asyncMessageSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) (rerr error) {

	for {

		select {
		case m := <-messages:
			if err := ams.client.Produce(m.Data()); err != nil {
				return err
			}
			select {
			case acks <- m:
			case <-ctx.Done():
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (ams *asyncMessageSink) Status() (*substrate.Status, error) {
	return &substrate.Status{Working: true}, nil
}

// Close implements the Close method of the substrate.AsyncMessageSink
// interface.
func (ams *asyncMessageSink) Close() error {
	return ams.client.Close()
}
