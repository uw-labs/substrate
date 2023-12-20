package franz

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/uwos-go/telemetry/log"
	fkafka "github.com/utilitywarehouse/uwos-go/x/pubsub/kafka"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/kafka"
)

type asyncMessageSink struct {
	client    *fkafka.Client
	admClient *kadm.Client

	cfg kafka.AsyncMessageSinkConfig
}

func NewAsyncMessageSink(cfg kafka.AsyncMessageSinkConfig) (substrate.AsyncMessageSink, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
	}
	if cfg.MaxMessageBytes > 0 {
		opts = append(opts, kgo.ProducerBatchMaxBytes(int32(cfg.MaxMessageBytes)))
	}

	if cfg.MaxMessageBytes > 100*1024*1024 { // if this exceeds the 100MB also bump the BrokerMaxWriteBytes
		opts = append(opts, kgo.BrokerMaxWriteBytes(int32(cfg.MaxMessageBytes)))
	}

	if cfg.Debug {
		logOpts := log.DefaultOptions()
		logOpts.Level = slog.LevelDebug
		opts = append(opts, fkafka.WithLogger(logOpts.New()))
	}

	//   - version: we don't have a 1/1 matching in franz-go. If you used it for a good reason, migrate to the uwos-go provided client and use [kgo.MinVersions] and [kgo.MaxVersions]
	c, err := fkafka.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed creating franz-go client: %w", err)
	}
	return &asyncMessageSink{client: c, admClient: kadm.NewClient(c.Client), cfg: cfg}, nil
}

func (s *asyncMessageSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-messages:
			/*	not effective, as this is actually synchronous. If you need real async sink you'll need another implementation */
			pr := s.client.ProduceSync(ctx, s.toRecord(msg))
			if pr.FirstErr() != nil {
				return pr.FirstErr()
			}

			select {
			case acks <- msg:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (s *asyncMessageSink) Close() error {
	s.client.Close()
	return nil
}

func (s *asyncMessageSink) toRecord(message substrate.Message) *kgo.Record {
	r := &kgo.Record{
		Value: message.Data(),
	}

	switch msg := message.(type) {
	case substrate.KeyedMessage:
		r.Key = msg.Key()
	default:
		if s.cfg.KeyFunc != nil {
			r.Key = s.cfg.KeyFunc(message)
		}
	}
	return r
}

func (s *asyncMessageSink) Status() (*substrate.Status, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelFunc()

	meta, err := s.admClient.Metadata(ctx, s.cfg.Topic)
	if err != nil {
		return statusWithError(err), nil
	}

	td := meta.Topics[s.cfg.Topic]
	if td.Err != nil {
		return statusWithError(td.Err), nil
	}

	for _, pd := range td.Partitions {
		if pd.Err != nil {
			return statusWithError(pd.Err), nil
		}
	}

	return &substrate.Status{Working: true}, nil
}

func statusWithError(err error) *substrate.Status {
	return &substrate.Status{
		Working:  false,
		Problems: []string{err.Error()},
	}
}
