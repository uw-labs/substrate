package instrumented

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/uw-labs/substrate"
)

type asyncMessageSinkMock struct {
	substrate.AsyncMessageSink
	publishMessageMock func(context.Context, chan<- substrate.Message, <-chan substrate.Message) error
}

func (m asyncMessageSinkMock) PublishMessages(ctx context.Context, acks chan<- substrate.Message, out <-chan substrate.Message) error {
	return m.publishMessageMock(ctx, acks, out)
}

type Message struct {
	data []byte
}

func (m Message) Data() []byte {
	return m.data
}

func TestPublishMessagesSuccessfully(t *testing.T) {
	sink := AsyncMessageSink{
		impl: &asyncMessageSinkMock{
			publishMessageMock: func(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) error {
				for {
					select {
					case <-ctx.Done():
						return nil
					case msg := <-messages:
						acks <- msg
					}
				}
			},
		},
		counter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Help: "sink_counter",
				Name: "sink_counter",
			}, []string{"status", "topic"}),
		topic: "testTopic",
	}

	acks := make(chan substrate.Message)
	messages := make(chan substrate.Message)

	sinkContext, sinkCancel := context.WithCancel(context.Background())
	defer sinkCancel()

	errs := make(chan error)
	go func() {
		defer close(errs)
		errs <- sink.PublishMessages(sinkContext, acks, messages)
	}()

	messages <- Message{}

	for {
		select {
		case err := <-errs:
			assert.NoError(t, err)
			return
		case <-acks:
			var metric dto.Metric
			sink.counter.WithLabelValues("success", "testTopic").Write(&metric)
			assert.Equal(t, 1, int(*metric.Counter.Value))
			sinkCancel()
		}
	}
}

func TestPublishMessagesWithError(t *testing.T) {
	producingError := errors.New("message producing error")
	sink := AsyncMessageSink{
		impl: &asyncMessageSinkMock{
			publishMessageMock: func(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) error {
				for {
					select {
					case <-ctx.Done():
						return nil
					case <-messages:
						return producingError
					}
				}
			},
		},
		counter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Help: "sink_counter",
				Name: "sink_counter",
			}, []string{"status", "topic"}),
		topic: "testTopic",
	}

	acks := make(chan substrate.Message)
	messages := make(chan substrate.Message)

	sinkContext, sinkCancel := context.WithCancel(context.Background())
	defer sinkCancel()

	errs := make(chan error)
	go func() {
		defer close(errs)
		errs <- sink.PublishMessages(sinkContext, acks, messages)
	}()

	messages <- Message{}

	for {
		select {
		case err := <-errs:
			assert.Error(t, err)
			assert.Equal(t, producingError, err)

			var metric dto.Metric
			sink.counter.WithLabelValues("error", "testTopic").Write(&metric)
			assert.Equal(t, 1, int(*metric.Counter.Value))

			sinkCancel()
			return
		case <-acks:
			t.Fatal("No message should be acknowldged")
		}
	}
}
