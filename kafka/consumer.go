package kafka

import (
	"context"
	"io"
	"time"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-multierror"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/internal/debug"
	"github.com/uw-labs/sync/rungroup"
)

const (
	// OffsetOldest indicates the oldest appropriate message available on the broker.
	OffsetOldest int64 = sarama.OffsetOldest
	// OffsetNewest indicates the next appropriate message available on the broker.
	OffsetNewest int64 = sarama.OffsetNewest

	defaultMetadataRefreshFrequency = 10 * time.Minute
	defaultConsumerSessionTimeout   = 10 * time.Second
)

// AsyncMessageSource represents a kafka message source and implements the
// substrate.AsyncMessageSource interface.
type AsyncMessageSourceConfig struct {
	ConsumerGroup            string
	Topic                    string
	Brokers                  []string
	Offset                   int64
	MetadataRefreshFrequency time.Duration
	OffsetsRetention         time.Duration
	SessionTimeout           time.Duration
	Version                  string
	SASL                     *SASLConfig

	Debug bool
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
	st := defaultConsumerSessionTimeout
	if ams.SessionTimeout != 0 {
		st = ams.SessionTimeout
	}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = offset
	config.Metadata.RefreshFrequency = mrf
	config.Consumer.Group.Session.Timeout = st
	config.Consumer.Offsets.Retention = ams.OffsetsRetention

	if ams.SASL != nil {
		config.Net.SASL.Enable = true
		config.Net.TLS.Enable = true
		config.Net.SASL.Mechanism = ams.SASL.Mechanism.sarama()
		config.Net.SASL.User = ams.SASL.Username
		config.Net.SASL.Password = ams.SASL.Password
		config.Net.SASL.Version = ams.SASL.Version
	}

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

		debugger: debug.Debugger{
			Enabled: c.Debug,
		},
	}, nil
}

type asyncMessageSource struct {
	client        sarama.Client
	consumerGroup sarama.ConsumerGroup
	topic         string

	debugger debug.Debugger
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

func (cm *consumerMessage) Key() []byte {
	if cm.cm == nil {
		panic("attempt to get the key after discarding.")
	}
	return cm.cm.Key
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
			debugger:    ams.debugger,
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
				debugger:    ams.debugger,
			})
			if err != nil {
				return err
			}
			if ctx.Err() != nil {
				return ctx.Err()
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
