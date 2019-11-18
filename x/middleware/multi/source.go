package backend

import (
	"context"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/sync/rungroup"
)

// Source implements substrate.AsyncMessageSource that consumes messages from multiple sources.
type Source struct {
	Sources []substrate.AsyncMessageSource
}

// ConsumeMessages starts to consume messages from all the underlying sources and forwards acknowledgements
// to the appropriate one. It terminates as soon as any of the underlying sources does or when the context is cancelled.
func (s Source) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	toSources := make([]chan<- substrate.Message, len(s.Sources))

	rg, ctx := rungroup.New(ctx)
	for i, source := range s.Sources {

		index, source := i, source
		sourceAcks := make(chan substrate.Message)
		sourceMsgs := make(chan substrate.Message)
		toSources[index] = sourceAcks

		// Annotate messages with the index of the source they come from
		rg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case msg := <-sourceMsgs:
					tMsg := &sourceMessage{
						index: index,
						msg:   msg,
					}
					select {
					case <-ctx.Done():
						return nil
					case messages <- tMsg:
					}
				}
			}
		})

		// Start consuming source.
		rg.Go(func() error {
			return source.ConsumeMessages(ctx, sourceMsgs, sourceAcks)
		})
	}
	// Forward acks to the correct source.
	rg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case msg := <-acks:
				tMsg, ok := msg.(*sourceMessage)
				if !ok {
					return errors.Errorf("unexpected message type: %T", msg)
				}
				ackChan := toSources[tMsg.index]
				select {
				case <-ctx.Done():
					return nil
				case ackChan <- tMsg.msg:
				}
			}
		}
	})

	return rg.Wait()
}

// Close closes all the underlying sources and returns all errors encountered.
func (s Source) Close() (err error) {
	for _, source := range s.Sources {
		err = multierror.Append(err, source.Close()).ErrorOrNil()
	}
	return err
}

// Status calls the status method on all underlying sources. It collects all errors encountered and
// only reports working status if all the underlying sources do.
func (s Source) Status() (status *substrate.Status, err error) {
	status = &substrate.Status{Working: true}

	for _, source := range s.Sources {
		sourceStatus, sourceErr := source.Status()
		if sourceErr != nil {
			status.Working = false
			err = multierror.Append(err, sourceErr)
		} else {
			status.Working = status.Working && sourceStatus.Working
			status.Problems = append(status.Problems, sourceStatus.Problems...)
		}
	}

	return status, err
}

type sourceMessage struct {
	index int
	msg   substrate.Message
}

func (m *sourceMessage) DiscardPayload() {
	if dMsg, ok := m.msg.(substrate.DiscardableMessage); ok {
		dMsg.DiscardPayload()
	}
}

func (m *sourceMessage) Data() []byte {
	return m.msg.Data()
}
