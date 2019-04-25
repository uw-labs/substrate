package freezer

import (
	"context"
	"time"

	"github.com/uw-labs/freezer"
	"github.com/uw-labs/straw"
	"github.com/uw-labs/substrate"
	"golang.org/x/sync/errgroup"
)

var (
	_ substrate.AsyncMessageSink   = (*asyncMessageSink)(nil)
	_ substrate.AsyncMessageSource = (*asyncMessageSource)(nil)
)

type AsyncMessageSinkConfig struct {
	FlushInterval time.Duration
	StreamStore   straw.StreamStore
	FreezerConfig freezer.MessageSinkConfig
}

func NewAsyncMessageSink(config AsyncMessageSinkConfig) (substrate.AsyncMessageSink, error) {
	fms, err := freezer.NewMessageSink(config.StreamStore, config.FreezerConfig)
	if err != nil {
		return nil, err
	}

	flushInterval := config.FlushInterval
	if flushInterval == 0 {
		flushInterval = time.Second
	}
	return &asyncMessageSink{
		flushInterval: flushInterval,
		fms:           fms,
	}, nil
}

type asyncMessageSink struct {
	flushInterval time.Duration
	fms           *freezer.MessageSink
}

func (ams *asyncMessageSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) (rerr error) {
	var toAck []substrate.Message
	t := time.NewTimer(0)
	if !t.Stop() {
		<-t.C
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case m := <-messages:
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			t.Reset(ams.flushInterval)
			if err := ams.fms.PutMessage(m.Data()); err != nil {
				return err
			}
			toAck = append(toAck, m)
		case <-t.C:
			if err := ams.fms.Flush(); err != nil {
				return err
			}
			for _, m := range toAck {
				select {
				case <-ctx.Done():
					return nil
				case acks <- m:
				}
			}
			toAck = toAck[0:0]
		}
		if len(toAck) > 1024 {
			if err := ams.fms.Flush(); err != nil {
				return err
			}
			for _, m := range toAck {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case acks <- m:
				}
			}
			toAck = toAck[0:0]
			t.Stop()
		}
	}
}

func (ams *asyncMessageSink) Status() (*substrate.Status, error) {
	return &substrate.Status{Working: true}, nil
}

// Close implements the Close method of the substrate.AsyncMessageSink
// interface.
func (ams *asyncMessageSink) Close() error {
	return ams.fms.Close()
}

// AsyncMessageSourceConfig is the configuration options for creating a new AsyncMessageSource
type AsyncMessageSourceConfig struct {
	StreamStore   straw.StreamStore
	FreezerConfig freezer.MessageSourceConfig
}

func NewAsyncMessageSource(c AsyncMessageSourceConfig) (substrate.AsyncMessageSource, error) {
	fms := freezer.NewMessageSource(c.StreamStore, c.FreezerConfig)
	ams := &asyncMessageSource{fms}
	return ams, nil
}

type asyncMessageSource struct {
	fms *freezer.MessageSource
}

func (ams *asyncMessageSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {

	eg, ctx := errgroup.WithContext(ctx)

	ackQ := make(chan substrate.Message)

	eg.Go(func() error {
		var forAcking []substrate.Message

		for {
			select {
			case m := <-ackQ:
				forAcking = append(forAcking, m)
			case ack := <-acks:
				switch {
				case len(forAcking) == 0:
					return substrate.InvalidAckError{
						Acked:    ack,
						Expected: nil,
					}
				case ack != forAcking[0]:
					return substrate.InvalidAckError{
						Acked:    ack,
						Expected: forAcking[0],
					}
				default:
					forAcking = forAcking[1:]
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	eg.Go(func() error {
		handler := func(data []byte) error {
			cm := &consumerMessage{data}
			select {
			case ackQ <- cm:
				select {
				case messages <- cm:
				case <-ctx.Done():
					return ctx.Err()
				}
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return ams.fms.ConsumeMessages(ctx, handler)
	})

	err := eg.Wait()
	if err == context.Canceled || err == context.DeadlineExceeded {
		err = nil
	}
	return err
}

func (ams *asyncMessageSource) Status() (*substrate.Status, error) {
	return &substrate.Status{Working: true}, nil
}

func (ams *asyncMessageSource) Close() error {
	return nil
}

type consumerMessage struct {
	data []byte
}

func (cm *consumerMessage) Data() []byte {
	return cm.data
}
