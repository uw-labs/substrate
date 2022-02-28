package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/internal/debug"
	"github.com/uw-labs/substrate/internal/helper"
	"github.com/uw-labs/substrate/internal/unwrap"
	"golang.org/x/sync/errgroup"
)

var (
	_ substrate.AsyncMessageSink   = (*asyncMessageSink)(nil)
	_ substrate.AsyncMessageSource = (*asyncMessageSource)(nil)
)

type AsyncMessageSinkConfig struct {
	Brokers         []string
	Topic           string
	MaxMessageBytes int
	KeyFunc         func(substrate.Message) []byte
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
		client:  client,
		Topic:   config.Topic,
		KeyFunc: config.KeyFunc,

		debugger: debug.Debugger{
			Enabled: config.Debug,
		},
	}
	return helper.NewAckOrderingSink(&sink), nil
}

type asyncMessageSink struct {
	client  sarama.Client
	Topic   string
	KeyFunc func(substrate.Message) []byte

	debugger debug.Debugger
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

				// Get original user message if wrapped
				unwrappedMsg := unwrap.Unwrap(m)
				if ams.KeyFunc != nil {
					// Provide original user message to the partition key function.
					message.Key = sarama.ByteEncoder(ams.KeyFunc(unwrappedMsg))
				} else {
					// No user specified key func, check for keyed message type
					if km, ok := unwrappedMsg.(substrate.KeyedMessage); ok {
						if key := km.Key(); key != nil {
							message.Key = sarama.ByteEncoder(key)
						}
					} else {
						// Use the whole message as the hash key
						message.Key = sarama.ByteEncoder(unwrappedMsg.Data())
					}
				}

				message.Metadata = m

				if _, ok := message.Metadata.(substrate.Message); !ok {
					logrus.Errorf("Message metadata does not contain substrate message: %+v", message)
					logrus.Errorf("Message body: %s", string(m.Data()))

					if km, ok := unwrappedMsg.(substrate.KeyedMessage); ok {
						if key := km.Key(); key != nil {
							logrus.Errorf("Message key: %s", string(key))
						}
					}

					return fmt.Errorf("bug: Substrate metadata precondition failure")
				}

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

	conf.Producer.Partitioner = sarama.NewHashPartitioner

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
