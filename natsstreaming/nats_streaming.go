package natsstreaming

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/stan.go"
	"github.com/uw-labs/substrate"
)

var (
	_ substrate.AsyncMessageSink   = (*asyncMessageSink)(nil)
	_ substrate.AsyncMessageSource = (*asyncMessageSource)(nil)
)

const (
	// OffsetOldest indicates the oldest appropriate message available on the broker.
	OffsetOldest int64 = -2
	// OffsetNewest indicates the next appropriate message available on the broker.
	OffsetNewest int64 = -1
)

type connectionTimeOutConfig struct {
	seconds int
	tries   int
}

// AsyncMessageSinkConfig is the configuration parameters for an
// AsyncMessageSink.
type AsyncMessageSinkConfig struct {
	URL       string
	ClusterID string
	ClientID  string
	Subject   string

	// number in seconds between pings (min 1)
	ConnectionPingInterval int

	// the client will return an error after this many pings have timed out (min 3)
	ConnectionNumPings int
}

func NewAsyncMessageSink(config AsyncMessageSinkConfig) (substrate.AsyncMessageSink, error) {
	sink := asyncMessageSink{subject: config.Subject, connectionLost: make(chan error, 1)}

	clientID := config.ClientID
	if clientID == "" {
		clientID = generateID()
	}
	if config.ConnectionPingInterval < 1 {
		config.ConnectionPingInterval = 1
	}

	if config.ConnectionNumPings < 3 {
		config.ConnectionNumPings = 3
	}

	sc, err := stan.Connect(
		config.ClusterID, clientID, stan.NatsURL(config.URL),
		stan.Pings(config.ConnectionPingInterval, config.ConnectionNumPings),
		stan.SetConnectionLostHandler(func(_ stan.Conn, e error) {
			sink.connectionLost <- e
		}),
	)
	if err != nil {
		return nil, err
	}
	sink.sc = sc
	return &sink, nil
}

type asyncMessageSink struct {
	subject        string
	sc             stan.Conn // nats streaming
	connectionLost chan error
}

func (p *asyncMessageSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) (rerr error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	conn := p.sc

	natsAckErrs := make(chan error, 1)
	publishErr := make(chan error, 1)

	go func() {
	LOOP:
		for {
			select {
			case <-ctx.Done():
				//return ctx.Err()
				break LOOP
			case msg := <-messages:
				_, err := conn.PublishAsync(p.subject, msg.Data(), func(guid string, err error) {
					if err != nil {
						select {
						case natsAckErrs <- err:
						default:
						}
						return
					}
					acks <- msg
				})
				if err != nil {
					publishErr <- err
					break LOOP
				}
			}
		}
	}()
	select {
	case <-ctx.Done():
		return
	case ne := <-natsAckErrs:
		return ne
	case pe := <-publishErr:
		return pe
	case cle := <-p.connectionLost:
		return cle
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
	Offset      int64

	// number in seconds between pings (min 1)
	ConnectionPingInterval int

	// the client will return an error after this many pings have timed out (min 3)
	ConnectionNumPings int
}

type asyncMessageSource struct {
	conn         stan.Conn
	conf         AsyncMessageSourceConfig
	disconnected <-chan error
}

func NewAsyncMessageSource(c AsyncMessageSourceConfig) (substrate.AsyncMessageSource, error) {
	clientID := c.ClientID
	if clientID == "" {
		clientID = c.QueueGroup + generateID()
	}
	switch {
	case c.Offset == 0:
		c.Offset = OffsetNewest
	case c.Offset < -2:
		return nil, fmt.Errorf("invalid offset: '%v'", c.Offset)
	}

	if c.ConnectionPingInterval < 1 {
		c.ConnectionPingInterval = 1
	}

	if c.ConnectionNumPings < 3 {
		c.ConnectionNumPings = 3
	}

	disconnected := make(chan error, 1)
	conn, err := stan.Connect(c.ClusterID, clientID, stan.NatsURL(c.URL),
		stan.Pings(c.ConnectionPingInterval, c.ConnectionNumPings),
		stan.SetConnectionLostHandler(func(_ stan.Conn, e error) {
			disconnected <- e
			close(disconnected)
		}))
	if err != nil {
		return nil, err
	}
	return &asyncMessageSource{conn: conn, conf: c, disconnected: disconnected}, nil
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
	var offsetOpt stan.SubscriptionOption
	switch offset := c.conf.Offset; offset {
	case OffsetOldest:
		offsetOpt = stan.DeliverAllAvailable()
	case OffsetNewest:
		offsetOpt = stan.StartWithLastReceived()
	default:
		offsetOpt = stan.StartAtSequence(uint64(offset))
	}

	sub, err := c.conn.QueueSubscribe(
		c.conf.Subject,
		c.conf.QueueGroup,
		f,
		offsetOpt,
		stan.DurableName(c.conf.QueueGroup),
		stan.SetManualAckMode(),
		stan.AckWait(ackWait),
		stan.MaxInflight(maxInflight),
	)
	if err != nil {
		return err
	}

	err = handleAcks(ctx, msgsToAck, acks, c.disconnected)

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

func handleAcks(ctx context.Context, msgsToAck chan *consumerMessage, acks <-chan substrate.Message, disconnected <-chan error) error {
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
		case e, ok := <-disconnected:
			if ok {
				return e
			}
			return errors.New("nats connection no longer active, exiting ack loop")
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
