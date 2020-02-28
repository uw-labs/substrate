package kafka

import (
	"context"
	"io"
	"time"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-multierror"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/internal/unwrap"
	"github.com/uw-labs/sync/rungroup"
)

var (
	_ substrate.AsyncMessageSink   = (*asyncMessageSink)(nil)
	_ substrate.AsyncMessageSource = (*asyncMessageSource)(nil)
)

const (
	// OffsetOldest indicates the oldest appropriate message available on the broker.
	OffsetOldest int64 = sarama.OffsetOldest
	// OffsetNewest indicates the next appropriate message available on the broker.
	OffsetNewest int64 = sarama.OffsetNewest

	defaultMetadataRefreshFrequency = 10 * time.Minute
)

type AsyncMessageSinkConfig struct {
	Brokers         []string
	Topic           string
	MaxMessageBytes int
	KeyFunc         func(substrate.Message) []byte
	Version         string
}

func NewAsyncMessageSink(config AsyncMessageSinkConfig) (substrate.AsyncMessageSink, error) {

	conf, err := config.buildSaramaProducerConfig()
	if err != nil {
		return nil, err
	}

	client, err := sarama.NewClient(config.Brokers, conf)
	if err != nil {
		return nil, err
	}

	sink := asyncMessageSink{
		client:  client,
		Topic:   config.Topic,
		KeyFunc: config.KeyFunc,
	}
	return &sink, nil
}

type asyncMessageSink struct {
	client  sarama.Client
	Topic   string
	KeyFunc func(substrate.Message) []byte
}

func (ams *asyncMessageSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) (rerr error) {

	producer, err := sarama.NewAsyncProducerFromClient(ams.client)
	if err != nil {
		return err
	}

	err = ams.doPublishMessages(ctx, producer, acks, messages)
	if err != nil {
		_ = producer.Close()
		return err
	}
	return producer.Close()
}

func (ams *asyncMessageSink) doPublishMessages(ctx context.Context, producer sarama.AsyncProducer, acks chan<- substrate.Message, messages <-chan substrate.Message) (rerr error) {

	input := producer.Input()
	errs := producer.Errors()
	successes := producer.Successes()

	go func() {
		for suc := range successes {
			acks <- suc.Metadata.(substrate.Message)
		}
	}()
	for {
		select {
		case m := <-messages:
			message := &sarama.ProducerMessage{
				Topic: ams.Topic,
			}

			message.Value = sarama.ByteEncoder(m.Data())

			if ams.KeyFunc != nil {
				// Provide original user message to the partition key function.
				unwrappedMsg := unwrap.Unwrap(m)
				message.Key = sarama.ByteEncoder(ams.KeyFunc(unwrappedMsg))
			}

			message.Metadata = m
			input <- message
		case <-ctx.Done():
			return nil
		case err := <-errs:
			return err
		}
	}
}

func (ams *asyncMessageSink) Status() (*substrate.Status, error) {
	return status(ams.client, ams.Topic)
}

func (ams *AsyncMessageSinkConfig) buildSaramaProducerConfig() (*sarama.Config, error) {
	conf := sarama.NewConfig()
	conf.Producer.RequiredAcks = sarama.WaitForAll // make configurable
	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true
	conf.Producer.Retry.Max = 3
	conf.Producer.Timeout = time.Duration(60) * time.Second

	if ams.MaxMessageBytes != 0 {
		if ams.MaxMessageBytes > int(sarama.MaxRequestSize) {
			sarama.MaxRequestSize = int32(ams.MaxMessageBytes)
		}
		conf.Producer.MaxMessageBytes = int(ams.MaxMessageBytes)
	}

	if ams.KeyFunc != nil {
		conf.Producer.Partitioner = sarama.NewHashPartitioner
	} else {
		conf.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	}

	if ams.Version != "" {
		version, err := sarama.ParseKafkaVersion(ams.Version)
		if err != nil {
			return nil, err
		}
		conf.Version = version
	}

	return conf, nil
}

// Close implements the Close method of the substrate.AsyncMessageSink
// interface.
func (ams *asyncMessageSink) Close() error {
	return ams.client.Close()
}

// AsyncMessageSource represents a kafka message source and implements the
// substrate.AsyncMessageSource interface.
type AsyncMessageSourceConfig struct {
	ConsumerGroup            string
	Topic                    string
	Brokers                  []string
	Offset                   int64
	MetadataRefreshFrequency time.Duration
	OffsetsRetention         time.Duration
	Version                  string
}

func (ams *AsyncMessageSourceConfig) buildSaramaConsumerConfig() (*sarama.Config, error) {
	offset := OffsetNewest
	if ams.Offset != 0 {
		offset = ams.Offset
	}
	mrf := defaultMetadataRefreshFrequency
	if ams.MetadataRefreshFrequency > 0 {
		mrf = ams.MetadataRefreshFrequency
	}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = offset
	config.Metadata.RefreshFrequency = mrf
	config.Consumer.Offsets.Retention = ams.OffsetsRetention

	if ams.Version != "" {
		version, err := sarama.ParseKafkaVersion(ams.Version)
		if err != nil {
			return nil, err
		}
		config.Version = version
	}

	return config, nil
}

func NewAsyncMessageSource(c AsyncMessageSourceConfig) (substrate.AsyncMessageSource, error) {
	config, err := c.buildSaramaConsumerConfig()
	if err != nil {
		return nil, err
	}

	client, err := sarama.NewClient(c.Brokers, config)
	if err != nil {
		return nil, err
	}
	consumerGroup, err := sarama.NewConsumerGroupFromClient(c.ConsumerGroup, client)
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	return &asyncMessageSource{
		client:        client,
		consumerGroup: consumerGroup,
		topic:         c.Topic,
	}, nil
}

type asyncMessageSource struct {
	client        sarama.Client
	consumerGroup sarama.ConsumerGroup
	topic         string
}

type consumerMessage struct {
	cm *sarama.ConsumerMessage

	discard bool
	offset  *struct {
		topic     string
		partition int32
		offset    int64
	}
}

func (cm *consumerMessage) Data() []byte {
	if cm.cm == nil {
		panic("attempt to use payload after discarding.")
	}
	return cm.cm.Value
}

func (cm *consumerMessage) DiscardPayload() {
	if cm.offset != nil {
		// already discarded
		return
	}
	cm.offset = &struct {
		topic     string
		partition int32
		offset    int64
	}{
		cm.cm.Topic,
		cm.cm.Partition,
		cm.cm.Offset,
	}
	cm.cm = nil
}

type consumerGroupHandler struct {
	ctx         context.Context
	toAck       chan<- *consumerMessage
	sessCh      chan<- sarama.ConsumerGroupSession
	rebalanceCh chan<- struct{}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (c *consumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	// send session to the ack processor
	select {
	case <-c.ctx.Done():
	case c.sessCh <- sess:
	}
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
func (c *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	// signal to ack processor that rebalance might be happening
	select {
	case <-c.ctx.Done():
	case c.rebalanceCh <- struct{}{}:
	}
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (c *consumerGroupHandler) ConsumeClaim(_ sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// This function can be called concurrently for multiple claims, so the code
	// below, absent locking etc may seem wrong, but it's actually fine.
	// Different partition claims can be processed concurrently, but we funnel
	// them all into c.toAck, which is consumed and processed by a single goroutine.
	for {
		select {
		case <-c.ctx.Done():
			return nil
		case m, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			cm := &consumerMessage{cm: m}
			select {
			case c.toAck <- cm:
			case <-c.ctx.Done():
				return nil
			}
		}
	}
}

type kafkaAcksProcessor struct {
	toClient    chan<- substrate.Message
	fromKafka   <-chan *consumerMessage
	acks        <-chan substrate.Message
	sessCh      <-chan sarama.ConsumerGroupSession
	rebalanceCh <-chan struct{}

	sess      sarama.ConsumerGroupSession
	forAcking []*consumerMessage
}

func (ap *kafkaAcksProcessor) run(ctx context.Context) error {
	// First set session, so that we can acknowledge messages.
	select {
	case <-ctx.Done():
		return nil
	case ap.sess = <-ap.sessCh:
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ap.rebalanceCh:
			// Mark all pending messages to be discarded, as rebalance happened.
			for _, msg := range ap.forAcking {
				msg.discard = true
			}
			// Wait for the new session.
			select {
			case <-ctx.Done():
				return nil
			case ap.sess = <-ap.sessCh:
			}
		case msg := <-ap.fromKafka:
			if err := ap.processMessage(ctx, msg); err != nil {
				if err == context.Canceled {
					// This error is returned when a context cancellation is encountered
					// before the message was sent to the client so, we just return nil,
					// as we do in other context cancellation cases.
					return nil
				}
				return err
			}
		case ack := <-ap.acks:
			if err := ap.processAck(ack); err != nil {
				return err
			}
		}
	}
}

func (ap *kafkaAcksProcessor) processMessage(ctx context.Context, msg *consumerMessage) error {
	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		case <-ap.rebalanceCh:
			// Mark all pending messages to be discarded, as rebalance happened.
			for _, msg := range ap.forAcking {
				msg.discard = true
			}
			// Wait for the new session.
			select {
			case <-ctx.Done():
			case ap.sess = <-ap.sessCh:
			}
			return nil // We can return immediately as the current message can be discarded.
		case ap.toClient <- msg:
			ap.forAcking = append(ap.forAcking, msg)
			return nil // We have passed the message to the client, so we can exit this loop.
		case ack := <-ap.acks:
			// Still process acks, so that we don't block the consumer acknowledging the message.
			if err := ap.processAck(ack); err != nil {
				return err
			}
		}
	}
}

func (ap *kafkaAcksProcessor) processAck(ack substrate.Message) error {
	switch {
	case len(ap.forAcking) == 0:
		return substrate.InvalidAckError{
			Acked:    ack,
			Expected: nil,
		}
	case ack != ap.forAcking[0]:
		return substrate.InvalidAckError{
			Acked:    ack,
			Expected: ap.forAcking[0],
		}
	case ap.forAcking[0].discard:
		// Discard pending message that was consumed before a rebalance.
		ap.forAcking = ap.forAcking[1:]
	default:
		// Acknowledge the message.
		if ap.forAcking[0].cm != nil {
			ap.sess.MarkMessage(ap.forAcking[0].cm, "")
		} else {
			off := ap.forAcking[0].offset
			// MarkOffset marks the next message to consume, so we need to add 1
			// to the offset to mark this message as consumed. Note that the bsm cluster
			// did this when committing offsets, so that's why it worked without this before.
			ap.sess.MarkOffset(off.topic, off.partition, off.offset+1, "")
		}
		ap.forAcking = ap.forAcking[1:]
	}
	return nil
}

func (ams *asyncMessageSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	rg, ctx := rungroup.New(ctx)
	toAck := make(chan *consumerMessage)
	sessCh := make(chan sarama.ConsumerGroupSession)
	rebalanceCh := make(chan struct{})

	rg.Go(func() error {
		ap := &kafkaAcksProcessor{
			toClient:    messages,
			fromKafka:   toAck,
			acks:        acks,
			sessCh:      sessCh,
			rebalanceCh: rebalanceCh,
		}
		return ap.run(ctx)
	})
	rg.Go(func() error {
		// We need to run consume in infinite loop to handle rebalances.
		for {
			err := ams.consumerGroup.Consume(ctx, []string{ams.topic}, &consumerGroupHandler{
				ctx:         ctx,
				toAck:       toAck,
				sessCh:      sessCh,
				rebalanceCh: rebalanceCh,
			})
			if err != nil || ctx.Err() != nil {
				return err
			}
		}
	})

	return rg.Wait()
}

func (ams *asyncMessageSource) Status() (*substrate.Status, error) {
	return status(ams.client, ams.topic)
}

func (ams *asyncMessageSource) Close() (err error) {
	for _, closer := range []io.Closer{ams.consumerGroup, ams.client} {
		err = multierror.Append(err, closer.Close()).ErrorOrNil()
	}
	return err
}
