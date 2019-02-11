package proximo

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	proximoc "github.com/uw-labs/proximo/proximoc-go"
	"github.com/uw-labs/substrate"
)

const (
	// OffsetOldest indicates the oldest appropriate message available on the broker.
	OffsetOldest int64 = 1
	// OffsetNewest indicates the next appropriate message available on the broker.
	OffsetNewest int64 = 2
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
	//	Offset        int64 TODO: offset for proximo
	Insecure bool
}

func NewAsyncMessageSource(c AsyncMessageSourceConfig) (substrate.AsyncMessageSource, error) {

	conn, err := dialProximo(c.Broker, c.Insecure)
	if err != nil {
		return nil, err
	}

	return &asyncMessageSource{
		conn:          conn,
		consumerGroup: c.ConsumerGroup,
		topic:         c.Topic,
	}, nil
}

type asyncMessageSource struct {
	conn          *grpc.ClientConn
	consumerGroup string
	topic         string
}

type consMsg struct {
	pm *proximoc.Message
}

func (cm consMsg) Data() []byte {
	return cm.pm.Data
}

func (ams *asyncMessageSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {

	eg, ctx := errgroup.WithContext(ctx)
	client := proximoc.NewMessageSourceClient(ams.conn)

	stream, err := client.Consume(ctx)
	if err != nil {
		return errors.Wrap(err, "fail to consume")
	}
	defer stream.CloseSend()

	if err := stream.Send(&proximoc.ConsumerRequest{
		StartRequest: &proximoc.StartConsumeRequest{
			Topic:    ams.topic,
			Consumer: ams.consumerGroup,
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
					if err := stream.Send(&proximoc.ConsumerRequest{Confirmation: &proximoc.Confirmation{MsgID: id}}); err != nil {
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
	return proximoStatus(ams.conn)
}

func (ams *asyncMessageSource) Close() error {
	return ams.conn.Close()
}
