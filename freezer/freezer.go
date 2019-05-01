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
	defaultMaxUnflushedCount  = 1024
	defaultMaxUnflushedPeriod = time.Second

	_ substrate.AsyncMessageSink   = (*asyncMessageSink)(nil)
	_ substrate.AsyncMessageSource = (*asyncMessageSource)(nil)
)

type AsyncMessageSinkConfig struct {
	StreamStore        straw.StreamStore
	FreezerConfig      freezer.MessageSinkConfig
	MaxUnflushedCount  int           // maximum number of unflushed messages
	MaxUnflushedPeriod time.Duration // maximum period before flushing
}

func NewAsyncMessageSink(config AsyncMessageSinkConfig) (substrate.AsyncMessageSink, error) {
	fms, err := freezer.NewMessageSink(config.StreamStore, config.FreezerConfig)
	if err != nil {
		return nil, err
	}
	if config.MaxUnflushedCount <= 0 {
		config.MaxUnflushedCount = defaultMaxUnflushedCount
	}
	if config.MaxUnflushedPeriod <= 0 {
		config.MaxUnflushedPeriod = defaultMaxUnflushedPeriod
	}

	return &asyncMessageSink{
		fms:                fms,
		maxUnflushedCount:  config.MaxUnflushedCount,
		maxUnflushedPeriod: config.MaxUnflushedPeriod,
	}, nil
}

type asyncMessageSink struct {
	fms                *freezer.MessageSink
	maxUnflushedCount  int
	maxUnflushedPeriod time.Duration
}

func (ams *asyncMessageSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) (rerr error) {
	var toAck []substrate.Message

	t := time.NewTicker(ams.maxUnflushedPeriod)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case m := <-messages:
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
		if len(toAck) >= ams.maxUnflushedCount {
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
