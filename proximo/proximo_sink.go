package proximo

import (
	"context"
	"io"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gofrs/uuid"
	"github.com/pkg/errors"

	proximoc "github.com/uw-labs/proximo/proximoc-go"
	"github.com/uw-labs/substrate"
)

var (
	_ substrate.AsyncMessageSink = (*asyncMessageSink)(nil)

	errStoppedReceivingAcks = errors.New("stopped receiving acknowledgements")
)

type AsyncMessageSinkConfig struct {
	Broker   string
	Topic    string
	Insecure bool
}

func NewAsyncMessageSink(config AsyncMessageSinkConfig) (substrate.AsyncMessageSink, error) {

	conn, err := grpc.Dial(config.Broker, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	sink := asyncMessageSink{
		conn:  conn,
		topic: config.Topic,
	}
	return &sink, nil
}

type asyncMessageSink struct {
	conn  *grpc.ClientConn
	topic string
}

func (ams *asyncMessageSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) (rerr error) {

	eg, ctx := errgroup.WithContext(ctx)

	client := proximoc.NewMessageSinkClient(ams.conn)
	stream, err := client.Publish(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to start publishing")
	}

	err = stream.Send(&proximoc.PublisherRequest{
		StartRequest: &proximoc.StartPublishRequest{
			Topic: ams.topic,
		},
	})
	if err != nil {
		return errors.Wrap(err, "failed to set publish topic")
	}

	toAck := make(chan *ackMessage)
	proximoAcks := make(chan string)

	eg.Go(func() error {
		return ams.sendMessagesToProximo(ctx, stream, messages, toAck)
	})
	eg.Go(func() error {
		return ams.receiveAcksFromProximo(ctx, stream, proximoAcks)
	})
	eg.Go(func() error {
		return ams.passAcksToUser(ctx, acks, toAck, proximoAcks)
	})

	if err = eg.Wait(); err != nil && err != errStoppedReceivingAcks {
		return err
	}
	return nil
}

type msgSendStream interface {
	Send(*proximoc.PublisherRequest) error
}

func (ams *asyncMessageSink) sendMessagesToProximo(ctx context.Context, stream msgSendStream, messages <-chan substrate.Message, toAck chan<- *ackMessage) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-messages:
			pMsg := &proximoc.Message{
				Id:   uuid.Must(uuid.NewV4()).String(),
				Data: msg.Data(),
			}
			select {
			case <-ctx.Done():
				return nil
			case toAck <- &ackMessage{id: pMsg.Id, msg: msg}:
			}
			if err := stream.Send(&proximoc.PublisherRequest{Msg: pMsg}); err != nil {
				return err
			}
		}
	}
}

type ackReceiverStream interface {
	Recv() (*proximoc.Confirmation, error)
}

func (ams *asyncMessageSink) receiveAcksFromProximo(ctx context.Context, stream ackReceiverStream, proximoAcks chan<- string) error {
	for {
		conf, err := stream.Recv()
		if err != nil {
			if err != io.EOF && status.Code(err) != codes.Canceled {
				return errors.Wrap(err, "failed to receive acknowledgement")
			}
			return errStoppedReceivingAcks
		}
		select {
		case <-ctx.Done():
			return nil
		case proximoAcks <- conf.MsgID:
		}
	}
}

func (ams *asyncMessageSink) passAcksToUser(ctx context.Context, acks chan<- substrate.Message, toAck <-chan *ackMessage, proximoAcks <-chan string) error {
	ackMap := make(map[string]substrate.Message)
	for {
		select {
		case <-ctx.Done():
			return nil
		case ack := <-toAck:
			ackMap[ack.id] = ack.msg
		case msgId := <-proximoAcks:
			msg, ok := ackMap[msgId]
			if !ok {
				return errors.New("received unexpected message confirmation from proximo")
			}
			select {
			case <-ctx.Done():
				return nil
			case acks <- msg:
			}
			delete(ackMap, msgId)
		}
	}
}

func (ams *asyncMessageSink) Status() (*substrate.Status, error) {
	return proximoStatus(ams.conn)
}

// Close implements the Close method of the substrate.AsyncMessageSink
// interface.
func (ams *asyncMessageSink) Close() error {
	return ams.conn.Close()
}

type ackMessage struct {
	id  string
	msg substrate.Message
}
