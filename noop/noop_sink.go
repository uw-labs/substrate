package noop

import (
	"context"

	"github.com/uw-labs/substrate"
)

// asyncMessageSink does no operations on publish messages calls
type asyncMessageSink struct{}

// NewAsyncMessageSink returns a pointer to a new asyncMessageSink
func NewAsyncMessageSink() substrate.AsyncMessageSink {
	return &asyncMessageSink{}
}

// PublishMessages implements message publishing by only acknowledging the messages
func (ams asyncMessageSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) error {
	for {
		select {
		case msg := <-messages:
			select {
			case acks <- msg:
			case <-ctx.Done():
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// Close closes the message sink
func (ams asyncMessageSink) Close() error {
	return nil
}

// Status returns the status of this sink (always working)
func (ams asyncMessageSink) Status() (*substrate.Status, error) {
	return &substrate.Status{Working: true}, nil
}
