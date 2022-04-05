package proximo

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/sync/rungroup"

	"github.com/uw-labs/substrate"
)

// Offset is the type used to specify the initial subscription offset
type Offset int64

const (
	// OffsetOldest indicates the oldest appropriate message available on the broker.
	OffsetOldest Offset = 1
	// OffsetNewest indicates the next appropriate message available on the broker.
	OffsetNewest Offset = 2
)

var _ substrate.AsyncMessageSource = (*asyncMessageSource)(nil)

// AsyncMessageSource represents a proximo message source and implements the
// substrate.AsyncMessageSource interface.
type AsyncMessageSourceConfig struct {
	ConsumerGroup  string
	Topic          string
	Broker         string
	Offset         Offset
	Insecure       bool
	KeepAlive      *KeepAlive
	MaxRecvMsgSize int
	Credentials    *Credentials
}

func NewAsyncMessageSource(c AsyncMessageSourceConfig) (substrate.AsyncMessageSource, error) {
	conn, err := dialProximo(dialConfig{
		broker:         c.Broker,
		insecure:       c.Insecure,
		keepAlive:      c.KeepAlive,
		maxRecvMsgSize: c.MaxRecvMsgSize,
	})
	if err != nil {
		return nil, err
	}

	return &asyncMessageSource{
		conn:          conn,
		consumerGroup: c.ConsumerGroup,
		topic:         c.Topic,
		offset:        c.Offset,
		credentials:   c.Credentials,
	}, nil
}

type asyncMessageSource struct {
	conn          *grpc.ClientConn
	consumerGroup string
	topic         string
	offset        Offset
	credentials   *Credentials
}

type consMsg struct {
	id string
	pm *proto.Message
}

func (cm *consMsg) Data() []byte {
	if cm.pm == nil {
		panic("attempt to use payload after discarding.")
	}
	return cm.pm.Data
}

func (cm *consMsg) DiscardPayload() {
	if cm.pm != nil {
		cm.id = cm.pm.GetId()
		cm.pm = nil
	}
}

func (cm *consMsg) getMsgID() string {
	if cm.pm != nil {
		return cm.pm.Id
	}
	return cm.id
}

func (ams *asyncMessageSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	rg, ctx := rungroup.New(setupAuthentication(ctx, ams.credentials))
	client := proto.NewMessageSourceClient(ams.conn)

	stream, err := client.Consume(ctx)
	if err != nil {
		return fmt.Errorf("fail to consume: %w", err)
	}

	var offset proto.Offset
	if ams.offset == OffsetOldest {
		offset = proto.Offset_OFFSET_OLDEST
	} else {
		offset = proto.Offset_OFFSET_NEWEST
	}
	if err := stream.Send(&proto.ConsumerRequest{
		StartRequest: &proto.StartConsumeRequest{
			Topic:         ams.topic,
			Consumer:      ams.consumerGroup,
			InitialOffset: offset,
		},
	}); err != nil {
		return err
	}

	toAck := make(chan *consMsg)

	rg.Go(func() error {
		defer stream.CloseSend()

		var toAckList []*consMsg
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
					id := toAckList[0].getMsgID()
					if err := stream.Send(&proto.ConsumerRequest{Confirmation: &proto.Confirmation{MsgID: id}}); err != nil {
						if err == io.EOF || status.Code(err) == codes.Canceled {
							if ctx.Err() != nil {
								return ctx.Err()
							}
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

	rg.Go(func() error {
		for {
			in, err := stream.Recv()
			if err != nil {
				if err == io.EOF || status.Code(err) == codes.Canceled {
					if ctx.Err() != nil {
						return ctx.Err()
					}
				}
				return err
			}

			m := &consMsg{pm: in}
			select {
			case toAck <- m:
			case <-ctx.Done():
				return ctx.Err()
			}
			select {
			case messages <- m:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	return rg.Wait()
}

func (ams *asyncMessageSource) Status() (*substrate.Status, error) {
	return proximoStatus(ams.conn)
}

func (ams *asyncMessageSource) Close() error {
	return ams.conn.Close()
}
