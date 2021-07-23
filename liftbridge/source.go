package liftbridge

import (
	"context"

	lift "github.com/liftbridge-io/go-liftbridge"
	"github.com/uw-labs/substrate"
)

type AsyncMessageSourceConfig struct {
	Brokers []string
	Topic   string
	Offset  int64
}

func NewAsyncMessageSource(cfg AsyncMessageSourceConfig) (substrate.AsyncMessageSource, error) {
	client, err := lift.Connect(cfg.Brokers)
	if err != nil {
		return nil, err
	}

	return &asyncMessageSource{
		client: client,
		topic:  cfg.Topic,
	}, nil
}

type asyncMessageSource struct {
	client lift.Client
	topic  string
	offset int64
}

func (ams *asyncMessageSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	subCh := make(chan *lift.Message)
	subErrCh := make(chan error, 1)

	if err := ams.client.Subscribe(ctx, ams.topic, func(msg *lift.Message, err error) {
		if err != nil {
			select {
			case <-ctx.Done():
			case subErrCh <- err:
			}
			return
		}
		select {
		case <-ctx.Done():
		case subCh <- msg:
		}

	}, lift.StartAtOffset(ams.offset)); err != nil {
		return err
	}

	var expectedAcks []substrate.Message
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-subErrCh:
			return err
		case msg := <-subCh:
			sMsgs := &message{
				data:   msg.Value(),
				offset: msg.Offset(),
			}
			expectedAcks = append(expectedAcks, sMsgs)
			select {
			case <-ctx.Done():
			case messages <- sMsgs:
			}
		case ack := <-acks:
			msg, ok := ack.(*message)
			if !ok {
				if len(expectedAcks) > 0 {
					return substrate.InvalidAckError{
						Acked:    ack,
						Expected: expectedAcks[0],
					}
				}
				return substrate.InvalidAckError{Acked: ack}
			}
			switch {
			case len(expectedAcks) == 0:
				return substrate.InvalidAckError{
					Acked:    ack,
					Expected: nil,
				}
			case expectedAcks[0] != msg:
				return substrate.InvalidAckError{
					Acked:    ack,
					Expected: expectedAcks[0],
				}
			default:
				expectedAcks = expectedAcks[1:]
			}
		}
	}
}

func (ams *asyncMessageSource) Close() error {
	return ams.client.Close()
}

func (ams *asyncMessageSource) Status() (*substrate.Status, error) {
	return &substrate.Status{Working: true}, nil
}

type message struct {
	data   []byte
	offset int64
}

func (m *message) Data() []byte {
	return m.data
}

func (m *message) Offset() int64 {
	return m.offset
}
