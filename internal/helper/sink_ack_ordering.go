package helper

import (
	"context"

	"github.com/uw-labs/substrate"
	"golang.org/x/sync/errgroup"
)

var _ substrate.AsyncMessageSink = (*AckOrderingSink)(nil)

func NewAckOrderingSink(sink substrate.AsyncMessageSink) *AckOrderingSink {
	return &AckOrderingSink{sink}
}

type AckOrderingSink struct {
	innerSink substrate.AsyncMessageSink
}

func (s *AckOrderingSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) error {
	eg, ctx := errgroup.WithContext(ctx)

	fromInner := make(chan substrate.Message, 1024)
	toInner := make(chan substrate.Message, 1024)

	needAcks := make(chan substrate.Message, 1024)

	eg.Go(func() error {
		return s.innerSink.PublishMessages(ctx, fromInner, toInner)
	})

	eg.Go(func() error {
		for {
			select {
			case m := <-messages:
				select {
				case needAcks <- m:
				case <-ctx.Done():
					return ctx.Err()
				}
				select {
				case toInner <- m:
				case <-ctx.Done():
					return ctx.Err()
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	eg.Go(func() error {
		needed := make([]substrate.Message, 0, 1024)
		gotAcks := make(map[substrate.Message]struct{}, 1024)
		for {
			select {
			case m := <-needAcks:
				needed = append(needed, m)
			case a, ok := <-fromInner:
				if !ok {
					panic("?")
				}
				gotAcks[a] = struct{}{}
			case <-ctx.Done():
				return ctx.Err()
			}

			for len(gotAcks) > 0 && len(needed) != 0 && contains(gotAcks, needed[0]) {
				select {
				case m := <-needAcks:
					needed = append(needed, m)
				case acks <- needed[0]:
					delete(gotAcks, needed[0])
					needed = needed[1:]
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	})

	if err := eg.Wait(); err != nil && err != context.Canceled {
		return err
	}
	return nil
}

func contains(msgs map[substrate.Message]struct{}, msg substrate.Message) bool {
	_, ok := msgs[msg]
	return ok
}

func (s *AckOrderingSink) Close() error {
	return s.innerSink.Close()
}

func (s *AckOrderingSink) Status() (*substrate.Status, error) {
	return s.innerSink.Status()
}
