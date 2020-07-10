package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/internal/debug"
	"github.com/uw-labs/substrate/internal/helper"
	"golang.org/x/sync/errgroup"
)

var (
	_ substrate.AsyncMessageSink   = (*asyncMessageSink)(nil)
	_ substrate.AsyncMessageSource = (*asyncMessageSource)(nil)
)

type Partitioner uint8

const (
	// PartitionerRoundRobin is used by default. It ensures that messages are
	// sunk to topic partitions in a round robin fashion.
	PartitionerRoundRobin Partitioner = iota
	// PartitionerHash should be used when you intend to provide a partition key
	// for messages.
	PartitionerHash
)

type AsyncMessageSinkConfig struct {
	Brokers         []string
	Topic           string
	MaxMessageBytes int
	Partitioner     Partitioner
	Version         string

	Debug bool
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
		partitioner: config.Partitioner,
		client:      client,
		Topic:       config.Topic,

		debugger: debug.Debugger{
			Enabled: config.Debug,
		},
	}
	return helper.NewAckOrderingSink(&sink), nil
}

type asyncMessageSink struct {
	partitioner Partitioner
	client      sarama.Client
	Topic       string
	debugger    debug.Debugger
}

func (ams *asyncMessageSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) error {
	producer, err := sarama.NewAsyncProducerFromClient(ams.client)
	if err != nil {
		return err
	}

	err = ams.doPublishMessages(ctx, producer, acks, messages)

	if closeErr := producer.Close(); closeErr != nil {
		return closeErr
	}

	return err
}

func (ams *asyncMessageSink) doPublishMessages(ctx context.Context, producer sarama.AsyncProducer, acks chan<- substrate.Message, messages <-chan substrate.Message) error {
	input := producer.Input()
	errs := producer.Errors()
	successes := producer.Successes()

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		for {
			select {
			case suc := <-successes:
				msg := suc.Metadata.(substrate.Message)
				select {
				case acks <- msg:
					ams.debugger.Logf("substrate : producer - sent ack to caller for message : %s\n", msg)
				case <-ctx.Done():
					return ctx.Err()
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	eg.Go(func() error {
		for {
			select {
			case m := <-messages:
				message := &sarama.ProducerMessage{
					Topic: ams.Topic,
				}

				message.Value = sarama.ByteEncoder(m.Data())

				if m.Key() != nil {
					if ams.partitioner != PartitionerHash {
						return fmt.Errorf("message with non-nil Key func, but sink using RoundRobin partitioner")
					}
					message.Key = sarama.ByteEncoder(m.Key())
				}

				message.Metadata = m
				select {
				case input <- message:
				case <-ctx.Done():
					return ctx.Err()
				}
				ams.debugger.Logf("substrate : producer - sent to kafka : %s\n", m)
			case <-ctx.Done():
				return ctx.Err()
			case err := <-errs:
				return err
			}
		}
	})

	return eg.Wait()
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

	switch ams.Partitioner {
	default:
		conf.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	case PartitionerHash:
		conf.Producer.Partitioner = sarama.NewHashPartitioner
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
