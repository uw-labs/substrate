package proximo

import (
	"context"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	proximo "github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

const (
	// OffsetOldest indicates the oldest appropriate message available on the broker.
	OffsetOldest int64 = -2
	// OffsetNewest indicates the next appropriate message available on the broker.
	OffsetNewest int64 = -1
)

var (
	_ substrate.AsyncMessageSource = (*asyncMessageSource)(nil)
)

// AsyncMessageSource represents a proximo message source and implements the
// substrate.AsyncMessageSource interface.
type AsyncMessageSourceConfig struct {
	ConsumerGroup string
	Topic         string
	Broker        string
	Offset        int64
	Insecure      bool
}

func NewAsyncMessageSource(c AsyncMessageSourceConfig) (substrate.AsyncMessageSource, error) {
	var offset proximo.Offset
	switch c.Offset {
	case 0:
		offset = proximo.OFFSET_DEFAULT
	case OffsetOldest:
		offset = proximo.OFFSET_OLDEST
	case OffsetNewest:
		offset = proximo.OFFSET_NEWEST
	default:
		return nil, fmt.Errorf("invalid offset: '%v'", c.Offset)
	}
	return &asyncMessageSource{
		broker:        c.Broker,
		consumerGroup: c.ConsumerGroup,
		topic:         c.Topic,
		insecure:      c.Insecure,
		offset:        offset,
	}, nil
}

type asyncMessageSource struct {
	broker        string
	consumerGroup string
	topic         string
	offset        proximo.Offset
	insecure      bool
}

type consMsg struct {
	pm *proximo.Message
}

func (cm consMsg) Data() []byte {
	return cm.pm.Data
}

func (ams *asyncMessageSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {

	eg, ctx := errgroup.WithContext(ctx)

	var opts []grpc.DialOption
	if ams.insecure {
		opts = append(opts, grpc.WithInsecure())
	}
	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*64)))

	conn, err := grpc.DialContext(ctx, ams.broker, opts...)
	if err != nil {
		return errors.Wrapf(err, "fail to dial %s", ams.broker)
	}
	defer conn.Close()

	client := proximo.NewMessageSourceClient(conn)

	stream, err := client.Consume(ctx)
	if err != nil {
		return errors.Wrap(err, "fail to consume")
	}

	defer stream.CloseSend()

	if err := stream.Send(&proximo.ConsumerRequest{
		StartRequest: &proximo.StartConsumeRequest{
			Topic:         ams.topic,
			Consumer:      ams.consumerGroup,
			InitialOffset: ams.offset,
		},
	}); err != nil {
		return err
	}

	toAck := make(chan substrate.Message)

	eg.Go(func() error {
		var toAckList []substrate.Message
		for {
			select {
			case ta := <-toAck:
				toAckList = append(toAckList, ta)
			case a := <-acks:
				switch {
				case len(toAckList) == 0:
					return substrate.InvalidAckError{Acked: a}
				case a != toAckList[0]:
					return substrate.InvalidAckError{Acked: a, Expected: toAckList[0]}
				default:
					id := toAckList[0].(consMsg).pm.Id
					if err := stream.Send(&proximo.ConsumerRequest{Confirmation: &proximo.Confirmation{MsgID: id}}); err != nil {
						if err == io.EOF || grpc.Code(err) == codes.Canceled {
							return nil
						}
						return err
					}
					toAckList = toAckList[1:]
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	eg.Go(func() error {
		for {
			in, err := stream.Recv()
			if err != nil {
				if err != io.EOF && grpc.Code(err) != codes.Canceled {
					return err
				}
				return nil
			}

			m := consMsg{in}
			select {
			case toAck <- m:
			case <-ctx.Done():
				return ctx.Err()
			}
			select {
			case messages <- consMsg{in}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	err = eg.Wait()
	if err == context.Canceled {
		return nil
	}
	return err

}

func (ams *asyncMessageSource) Status() (*substrate.Status, error) {
	return &substrate.Status{Working: true}, nil
}

func (ams *asyncMessageSource) Close() error {
	return nil
}
