package substrate

import (
	"context"

	"github.com/uw-labs/sync/rungroup"
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

	rg, ctx := rungroup.New(ctx)

	messages := make(chan Message)
	acks := make(chan Message)

	rg.Go(func() error {
		return a.ac.ConsumeMessages(ctx, messages, acks)
	})

	rg.Go(func() error {
		for {
			select {
			case msg := <-messages:
				if err := handler(ctx, msg); err != nil {
					return err
				}
				select {
				case acks <- msg:
				case <-ctx.Done():
					return ctx.Err()
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	return rg.Wait()
}

func (a *synchronousMessageSourceAdapter) Close() error {
	return a.ac.Close()
}

func (a *synchronousMessageSourceAdapter) Status() (*Status, error) {
	return a.ac.Status()
}
