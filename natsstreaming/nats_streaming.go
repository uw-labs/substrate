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
	_ substrate.AsyncMessageSink   = (*asyncMessageSink)(nil)
	_ substrate.AsyncMessageSource = (*asyncMessageSource)(nil)
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
	sink := asyncMessageSink{subject: config.Subject}

	clientID := config.ClientID
	if clientID == "" {
		clientID = generateID()
	}

	sc, err := stan.Connect(config.ClusterID, clientID, stan.NatsURL(config.URL))
	if err != nil {
		return nil, err
	}
	sink.sc = sc
	return &sink, nil
}

type asyncMessageSink struct {
	subject string
	sc      stan.Conn // nats streaming
}

func (p *asyncMessageSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) (rerr error) {

	conn := p.sc

	ackMap := make(map[string]substrate.Message)
	natsAcks := make(chan string, cap(messages))
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

func (p *asyncMessageSink) Close() error {
	return p.sc.Close()
}

func (p *asyncMessageSink) Status() (*substrate.Status, error) {
	return natsStatus(p.sc.NatsConn())
}

// AsyncMessageSource represents a nats-streaming message source and implements
// the substrate.AsyncMessageSource interface.
type AsyncMessageSourceConfig struct {
	URL         string
	ClusterID   string
	ClientID    string
	Subject     string
	QueueGroup  string
	MaxInFlight int
	AckWait     time.Duration
}

type asyncMessageSource struct {
	conn stan.Conn
	conf AsyncMessageSourceConfig
}

func NewAsyncMessageSource(c AsyncMessageSourceConfig) (substrate.AsyncMessageSource, error) {
	clientID := c.ClientID
	if clientID == "" {
		clientID = c.QueueGroup + generateID()
	}
	conn, err := stan.Connect(c.ClusterID, clientID, stan.NatsURL(c.URL))
	if err != nil {
		return nil, err
	}
	return &asyncMessageSource{conn, c}, nil
}

type consumerMessage struct {
	m *stan.Msg
}

func (cm *consumerMessage) Data() []byte {
	return cm.m.Data
}

func (c *asyncMessageSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {

	msgsToAck := make(chan *consumerMessage)

	f := func(msg *stan.Msg) {
		cm := &consumerMessage{msg}
		msgsToAck <- cm
		select {
		case <-ctx.Done():
			return
		case messages <- cm:
		}
	}

	maxInflight := c.conf.MaxInFlight
	if maxInflight == 0 {
		maxInflight = stan.DefaultMaxInflight
	}
	ackWait := c.conf.AckWait
	if ackWait == 0 {
		ackWait = stan.DefaultAckWait
	}

	sub, err := c.conn.QueueSubscribe(
		c.conf.Subject,
		c.conf.QueueGroup,
		f,
		stan.StartAt(pb.StartPosition_First),
		stan.DurableName(c.conf.QueueGroup),
		stan.SetManualAckMode(),
		stan.AckWait(ackWait),
		stan.MaxInflight(maxInflight),
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

func (ams *asyncMessageSource) Close() error {
	return ams.conn.Close()
}

func (ams *asyncMessageSource) Status() (*substrate.Status, error) {
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
			toAck = toAck[1:]
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
