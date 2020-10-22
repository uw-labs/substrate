package proximo

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gofrs/uuid"
	"github.com/pkg/errors"

	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/sync/rungroup"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/internal/debug"
)

var _ substrate.AsyncMessageSink = (*asyncMessageSink)(nil)

type AsyncMessageSinkConfig struct {
	Broker      string
	Topic       string
	Insecure    bool
	KeepAlive   *KeepAlive
	Credentials *Credentials
	Debug       bool
}

func NewAsyncMessageSink(c AsyncMessageSinkConfig) (substrate.AsyncMessageSink, error) {
	conn, err := dialProximo(dialConfig{
		broker:    c.Broker,
		insecure:  c.Insecure,
		keepAlive: c.KeepAlive,
	})
	if err != nil {
		return nil, err
	}

	return &asyncMessageSink{
		conn:        conn,
		topic:       c.Topic,
		credentials: c.Credentials,
		debugger: debug.Debugger{
			Enabled: c.Debug,
		},
	}, nil
}

type asyncMessageSink struct {
	conn        *grpc.ClientConn
	topic       string
	credentials *Credentials

	debugger debug.Debugger
}

func (ams *asyncMessageSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) (rerr error) {
	rg, ctx := rungroup.New(setupAuthentication(ctx, ams.credentials))

	client := proto.NewMessageSinkClient(ams.conn)
	stream, err := client.Publish(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to start publishing")
	}

	err = stream.Send(&proto.PublisherRequest{
		StartRequest: &proto.StartPublishRequest{
			Topic: ams.topic,
		},
	})
	if err != nil {
		return errors.Wrap(err, "failed to set publish topic")
	}

	toAck := make(chan *ackMessage)
	proximoAcks := make(chan string)

	rg.Go(func() error {
		defer stream.CloseSend()

		return ams.sendMessagesToProximo(ctx, stream, messages, toAck)
	})
	rg.Go(func() error {
		return ams.receiveAcksFromProximo(ctx, stream, proximoAcks)
	})
	rg.Go(func() error {
		return ams.passAcksToUser(ctx, acks, toAck, proximoAcks)
	})

	return rg.Wait()
}

type msgSendStream interface {
	Send(*proto.PublisherRequest) error
}

func (ams *asyncMessageSink) sendMessagesToProximo(ctx context.Context, stream msgSendStream, messages <-chan substrate.Message, toAck chan<- *ackMessage) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-messages:
			pMsg := &proto.Message{
				Id:   uuid.Must(uuid.NewV4()).String(),
				Data: msg.Data(),
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case toAck <- &ackMessage{id: pMsg.Id, msg: msg}:
			}
			if err := stream.Send(&proto.PublisherRequest{Msg: pMsg}); err != nil {
				if err == io.EOF || status.Code(err) == codes.Canceled {
					if ctx.Err() != nil {
						return ctx.Err()
					}
				}
				return errors.Wrap(err, "failed to send message to proximo")
			}
			ams.debugger.Logf("substrate : sent to proximo : %s which has id %s\n", pMsg, pMsg.Id)
		}
	}
}

type ackReceiverStream interface {
	Recv() (*proto.Confirmation, error)
}

func (ams *asyncMessageSink) receiveAcksFromProximo(ctx context.Context, stream ackReceiverStream, proximoAcks chan<- string) error {
	for {
		conf, err := stream.Recv()
		if err != nil {
			if err == io.EOF || status.Code(err) == codes.Canceled {
				if ctx.Err() != nil {
					return ctx.Err()
				}
			}
			return errors.Wrap(err, "failed to receive acknowledgement")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case proximoAcks <- conf.MsgID:
			ams.debugger.Logf("substrate : got ack msgid from proximo %s\n", conf.MsgID)
		}
	}
}

func (ams *asyncMessageSink) passAcksToUser(ctx context.Context, acks chan<- substrate.Message, toAck <-chan *ackMessage, proximoAcks <-chan string) error {
	ackMap := make(map[string]substrate.Message)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ack := <-toAck:
			ackMap[ack.id] = ack.msg
		case msgID := <-proximoAcks:
			msg, ok := ackMap[msgID]
			if !ok {
				return errors.New("received unexpected message confirmation from proximo")
			}
			sent := false
			for !sent {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ack := <-toAck:
					ackMap[ack.id] = ack.msg
				case acks <- msg:
					ams.debugger.Logf("substrate : sent ack to user : %v\n", msg)
					delete(ackMap, msgID)
					sent = true
				}
			}
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
