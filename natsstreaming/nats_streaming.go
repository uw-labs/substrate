package natsstreaming

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/uw-labs/substrate"
)

var (
	_ substrate.AsyncMessageSink   = (*AsyncMessageSink)(nil)
	_ substrate.AsyncMessageSource = (*AsyncMessageSource)(nil)
)

// AsyncMessageSinkConfig is the configarion parameters for an
// AsyncMessageSink.
type AsyncMessageSinkConfig struct {
	URL       string
	ClusterID string
	ClientID  string
	Subject   string
}

func NewAsyncMessageSink(config AsyncMessageSinkConfig) (substrate.AsyncMessageSink, error) {
	sink := AsyncMessageSink{subject: config.Subject}

	sc, err := stan.Connect(config.ClusterID, config.ClientID, stan.NatsURL(config.URL))
	if err != nil {
		return nil, err
	}
	sink.sc = sc
	return &sink, nil
}

// AsyncMessageSink represents a nats-streaming server and implements the
// substrate.AsyncMessageSink interface.
type AsyncMessageSink struct {
	subject string
	sc      stan.Conn // nats streaming
}

// PublishMessages implements the the PublishMessages method of the
// substrate.AsyncMessageSink interface.
func (p *AsyncMessageSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) (rerr error) {

	conn := p.sc

	ackMap := make(map[string]substrate.Message)
	natsAcks := make(chan string)
	natsAckErrs := make(chan error, 1)
	for {
		select {
		case <-ctx.Done():
			//return ctx.Err()
			return nil
		case msg := <-messages:
			guid, err := conn.PublishAsync(p.subject, msg.Data(), func(guid string, err error) {
				if err != nil {
					select {
					case natsAckErrs <- err:
					default:
					}
					return
				}
				natsAcks <- guid
			})
			if err != nil {
				return err
			}
			ackMap[guid] = msg

		case natsGUID := <-natsAcks:
			msg := ackMap[natsGUID]
			if msg == nil {
				return fmt.Errorf("got ack from nats streaming for unknown guid %v", natsGUID)
			}
			delete(ackMap, natsGUID)
			select {
			case acks <- msg:
			case <-ctx.Done():
				//return ctx.Err()
				return nil
			}
		case ne := <-natsAckErrs:
			return ne
		}
	}
}

// Close implements the Close method of the substrate.AsyncMessageSink
// interface.
func (p *AsyncMessageSink) Close() error {
	return p.sc.Close()
}

// Status implements the Status method of the substrate.AsyncMessageSink
// interface.
func (p *AsyncMessageSink) Status() (*substrate.Status, error) {
	return natsStatus(p.sc.NatsConn())
}

// AsyncMessageSource represents a nats-streaming message source and implements
// the substrate.AsyncMessageSource interface.
type AsyncMessageSourceConfig struct {
	url           string
	clusterID     string
	subject       string
	consumerGroup string
}

type AsyncMessageSource struct {
	conn stan.Conn
	conf AsyncMessageSourceConfig
}

func NewAsyncMessageSource(c AsyncMessageSourceConfig) (substrate.AsyncMessageSource, error) {
	conn, err := stan.Connect(c.clusterID, c.consumerGroup+generateID(), stan.NatsURL(c.url))
	if err != nil {
		return nil, err
	}
	return &AsyncMessageSource{conn, c}, nil
}

type consumerMessage struct {
	m *stan.Msg
}

func (cm *consumerMessage) Data() []byte {
	return cm.m.Data
}

// ConsumeMessages implements the the ConsumeMessages method of the
// substrate.AsyncMessageSource interface.
func (c *AsyncMessageSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {

	msgsToAck := make(chan *consumerMessage)

	f := func(msg *stan.Msg) {
		cm := &consumerMessage{msg}
		select {
		case <-ctx.Done():
			return
		case messages <- cm:
			msgsToAck <- cm
		}
	}

	sub, err := c.conn.QueueSubscribe(
		c.conf.subject,
		c.conf.consumerGroup,
		f,
		stan.StartAt(pb.StartPosition_First),
		stan.DurableName(c.conf.consumerGroup),
		stan.SetManualAckMode(),
		stan.AckWait(60*time.Second),
		stan.MaxInflight(32),
	)
	if err != nil {
		return err
	}

	err = handleAcks(ctx, msgsToAck, acks)

	se := sub.Close()
	if err == nil {
		err = se
	}

	return err
}

// Close implements the Close method of the substrate.AsyncMessageSource
// interface.
func (ams *AsyncMessageSource) Close() error {
	return ams.conn.Close()
}

// Status implements the Status method of the substrate.AsyncMessageSource
// interface.
func (ams *AsyncMessageSource) Status() (*substrate.Status, error) {
	return natsStatus(ams.conn.NatsConn())
}

func handleAcks(ctx context.Context, msgsToAck chan *consumerMessage, acks <-chan substrate.Message) error {
	var toAck []*consumerMessage

	for {
		select {
		case msgToAck := <-msgsToAck:
			toAck = append(toAck, msgToAck)
		case cr := <-acks:
			if len(toAck) == 0 {
				return substrate.InvalidAckError{Acked: cr, Expected: nil}
			}
			msgToAck := toAck[0]
			cm, ok := cr.(*consumerMessage)
			if !ok || cm != msgToAck {
				return substrate.InvalidAckError{Acked: cr, Expected: msgToAck}
			}
			if err := msgToAck.m.Ack(); err != nil {
				return fmt.Errorf("failed to ack message with NATS: %v", err.Error())
			}
		case <-ctx.Done():
			//return ctx.Err()
			return nil
		}
	}
}

func generateID() string {
	random := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	_, err := rand.Read(random)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(random)
}
