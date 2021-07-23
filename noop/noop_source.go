package noop

import (
	"context"

	"github.com/uw-labs/substrate"
)

// asyncMessageSource does no operations on publish messages calls
type asyncMessageSource struct{}

// NewAsyncMessageSource returns a new AsyncMessageSource
func NewAsyncMessageSource() substrate.AsyncMessageSource {
	return &asyncMessageSource{}
}

// ConsumeMessages implements message consumption by blocking until the provided context is cancelled.
func (ams asyncMessageSource) ConsumeMessages(ctx context.Context, _ chan<- substrate.Message, _ <-chan substrate.Message) error {
	<-ctx.Done()
	return ctx.Err()
}

// Close closes the message source
func (ams asyncMessageSource) Close() error {
	return nil
}

// Status returns the status of this source (always working)
func (ams asyncMessageSource) Status() (*substrate.Status, error) {
	return &substrate.Status{Working: true}, nil
}
