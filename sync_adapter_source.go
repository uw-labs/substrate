package substrate

import "context"

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

	ctx, cancel := context.WithCancel(ctx)

	messages := make(chan Message)
	acks := make(chan Message)

	errs := make(chan error, 1)

	go func() {
		errs <- a.ac.ConsumeMessages(ctx, messages, acks)
		close(errs)
	}()

	defer func() {
		cancel()
		<-errs
	}()

	for {
		select {
		case msg := <-messages:
			if err := handler(ctx, msg); err != nil {
				cancel()
				<-errs
				return err
			}
			select {
			case acks <- msg:
			case <-ctx.Done():
				return ctx.Err()
			}
		case err := <-errs:
			return err
		case <-ctx.Done():
			//return ctx.Err()
			return <-errs
		}
	}
}

func (a *synchronousMessageSourceAdapter) Close() error {
	return a.ac.Close()
}

func (a *synchronousMessageSourceAdapter) Status() (*Status, error) {
	return a.ac.Status()
}
