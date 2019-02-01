package substrate

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// NewSynchronousMessageSource returns a new synchronous message source, given
// an AsyncMessageSource. When Close is called on the SynchronousMessageSource,
// this is also propogated to the underlying SynchronousMessageSource.
func NewSynchronousMessageSource(ams AsyncMessageSource) SynchronousMessageSource {
	return &synchronousMessageSourceAdapter{
		ams,
	}
}

type synchronousMessageSourceAdapter struct {
	ac AsyncMessageSource
}

func (a *synchronousMessageSourceAdapter) ConsumeMessages(ctx context.Context, handler ConsumerMessageHandler) error {

	eg, ctx := errgroup.WithContext(ctx)

	messages := make(chan Message)
	acks := make(chan Message)

	eg.Go(func() error {
		return a.ac.ConsumeMessages(ctx, messages, acks)
	})

	eg.Go(func() error {
		for {
			select {
			case msg := <-messages:
				if err := handler(ctx, msg); err != nil {
					return err
				}
				select {
				case acks <- msg:
				case <-ctx.Done():
					return nil
				}
			case <-ctx.Done():
				return nil
			}
		}
	})

	return eg.Wait()
}

func (a *synchronousMessageSourceAdapter) Close() error {
	return a.ac.Close()
}

func (a *synchronousMessageSourceAdapter) Status() (*Status, error) {
	return a.ac.Status()
}
