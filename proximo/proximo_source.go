package proximo

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/uw-labs/proximo/proximoc-go"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/sync/rungroup"
)

// Offset is the type used to specify the initial subscription offset
type Offset int64

const (
	// OffsetOldest indicates the oldest appropriate message available on the broker.
	OffsetOldest Offset = 1
	// OffsetNewest indicates the next appropriate message available on the broker.
	OffsetNewest Offset = 2
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
	Offset        Offset
	Insecure      bool
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
	offset        Offset
}

type consMsg struct {
	id string
	pm *proximoc.Message
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

	rg, ctx := rungroup.New(ctx)
	client := proximoc.NewMessageSourceClient(ams.conn)

	stream, err := client.Consume(ctx)
	if err != nil {
		return errors.Wrap(err, "fail to consume")
	}

	var offset proximoc.Offset
	if ams.offset == OffsetOldest {
		offset = proximoc.Offset_OFFSET_OLDEST
	} else {
		offset = proximoc.Offset_OFFSET_NEWEST
	}
	if err := stream.Send(&proximoc.ConsumerRequest{
		StartRequest: &proximoc.StartConsumeRequest{
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
					if err := stream.Send(&proximoc.ConsumerRequest{Confirmation: &proximoc.Confirmation{MsgID: id}}); err != nil {
						if err == io.EOF || status.Code(err) == codes.Canceled {
							return nil
						}
						return err
					}
					toAckList = toAckList[1:]
				}
			case <-ctx.Done():
				return nil
			}
		}
	})

	rg.Go(func() error {
		for {
			in, err := stream.Recv()
			if err != nil {
				if err == io.EOF || status.Code(err) == codes.Canceled {
					return nil
				}
				return err
			}

			m := &consMsg{pm: in}
			select {
			case toAck <- m:
			case <-ctx.Done():
				return nil
			}
			select {
			case messages <- m:
			case <-ctx.Done():
				return nil
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
