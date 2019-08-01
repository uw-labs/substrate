package instrumented

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/uw-labs/substrate"
)

type asyncMessageSourceMock struct {
	substrate.AsyncMessageSource
	consumerMessagesMock func(context.Context, chan<- substrate.Message, <-chan substrate.Message) error
}

func (m asyncMessageSourceMock) ConsumeMessages(ctx context.Context, in chan<- substrate.Message, acks <-chan substrate.Message, opts ...substrate.Option) error {
	return m.consumerMessagesMock(ctx, in, acks)
}

func TestConsumeMessagesSuccessfully(t *testing.T) {
	receivedAcks := make(chan substrate.Message)

	source := AsyncMessageSource{
		impl: &asyncMessageSourceMock{
			consumerMessagesMock: func(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
				messages <- Message{}

				for {
					select {
					case <-ctx.Done():
						return nil
					case ack := <-acks:
						receivedAcks <- ack
					}
				}
			},
		},
		counter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Help: "source_counter",
				Name: "source_counter",
			}, []string{"status", "topic"}),
		topic: "testTopic",
	}

	acks := make(chan substrate.Message)
	messages := make(chan substrate.Message)

	sourceContext, sourceCancel := context.WithCancel(context.Background())
	defer sourceCancel()

	errs := make(chan error)
	go func() {
		defer close(errs)
		errs <- source.ConsumeMessages(sourceContext, messages, acks)
	}()

	for {
		select {
		case m := <-messages:
			acks <- m
		case err := <-errs:
			assert.NoError(t, err)
			return
		case <-receivedAcks:
			var metric dto.Metric
			assert.NoError(t, source.counter.WithLabelValues("success", "testTopic").Write(&metric))
			assert.Equal(t, 1, int(*metric.Counter.Value))

			sourceCancel()
		}
	}
}

func TestConsumeMessagesWithError(t *testing.T) {
	consumingErr := errors.New("consuming error")

	source := AsyncMessageSource{
		impl: &asyncMessageSourceMock{
			consumerMessagesMock: func(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
				return consumingErr
			},
		},
		counter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Help: "source_counter",
				Name: "source_counter",
			}, []string{"status", "topic"}),
		topic: "testTopic",
	}

	acks := make(chan substrate.Message)
	messages := make(chan substrate.Message, 1)

	sourceContext, sourceCancel := context.WithCancel(context.Background())
	defer sourceCancel()

	errs := make(chan error)
	go func() {
		defer close(errs)
		errs <- source.ConsumeMessages(sourceContext, messages, acks)
	}()

	err := <-errs
	assert.Error(t, err)
	assert.Equal(t, consumingErr, err)

	var metric dto.Metric
	assert.NoError(t, source.counter.WithLabelValues("error", "testTopic").Write(&metric))
	assert.Equal(t, 1, int(*metric.Counter.Value))

	sourceCancel()
}

func TestConsumeOnBackendShutdown(t *testing.T) {
	expectedErr := errors.New("shutdown")
	backendCtx, backendCancel := context.WithCancel(context.Background())

	source := AsyncMessageSource{
		impl: &asyncMessageSourceMock{
			consumerMessagesMock: func(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
				select {
				case <-ctx.Done():
					return nil
				case <-backendCtx.Done():
					return expectedErr
				}
			},
		},
		counter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Help: "source_counter",
				Name: "source_counter",
			}, []string{"status", "topic"}),
		topic: "testTopic",
	}

	acks := make(chan substrate.Message)
	messages := make(chan substrate.Message)

	sourceContext, sourceCancel := context.WithTimeout(context.Background(), time.Second*5)
	defer sourceCancel()

	errs := make(chan error)
	go func() {
		defer close(errs)
		errs <- source.ConsumeMessages(sourceContext, messages, acks)
	}()

	// Writer acknowledgement
	select {
	case <-sourceContext.Done():
		t.Fatalf("Failed to write acknowledgement.")
	case acks <- Message{}:
	}

	// Shutdown backend
	backendCancel()

	// Check wrapper shuts down properly
	select {
	case <-sourceContext.Done():
		t.Fatalf("Wrapper failed to shutdown.")
	case err := <-errs:
		assert.Equal(t, expectedErr, err)
		// Check metric was increased
		var metric dto.Metric
		assert.NoError(t, source.counter.WithLabelValues("error", "testTopic").Write(&metric))
		assert.Equal(t, 1, int(*metric.Counter.Value))
	}
}
