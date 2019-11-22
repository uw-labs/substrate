package noop

import (
	"context"

	"github.com/uw-labs/substrate"
)

// AsyncMessageSink does no operations on publish messages calls
type AsyncMessageSink struct{}

// NewAsyncMessageSink returns a pointer to a new AsyncMessageSink
func NewAsyncMessageSink() (substrate.AsyncMessageSink, error) {
	return &AsyncMessageSink{}, nil
}

// PublishMessages implements message publshing wrapped in instrumentation
func (ams *AsyncMessageSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) (rerr error) {
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
func (ams *AsyncMessageSink) Close() error {
	return nil
}

// Status returns the status of this sink, or an error if the status could not be determined.
func (ams *AsyncMessageSink) Status() (*substrate.Status, error) {
	return &substrate.Status{Working: true}, nil
}
