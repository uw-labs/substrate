package instrumented

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/uw-labs/substrate"
)

// AsyncMessageSource is an instrumented message source
// The counter vector will have the labels "status" and "topic"
type AsyncMessageSource struct {
	impl    substrate.AsyncMessageSource
	counter *prometheus.CounterVec
	topic   string
}

// NewAsyncMessageSource returns a pointer to a new AsyncMessageSource
func NewAsyncMessageSource(source substrate.AsyncMessageSource, counterOpts prometheus.CounterOpts, topic string) *AsyncMessageSource {
	counter := prometheus.NewCounterVec(counterOpts, labels)

	if err := prometheus.Register(counter); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			counter = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			panic(err)
		}
	}

	return &AsyncMessageSource{
		impl:    source,
		counter: counter,
		topic:   topic,
	}
}

// ConsumeMessages implements message consuming wrapped in instrumentation
func (ams *AsyncMessageSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	toBeAcked := make(chan substrate.Message, cap(acks))

	errs := make(chan error)
	go func() {
		defer close(errs)
		errs <- ams.impl.ConsumeMessages(ctx, messages, toBeAcked)
	}()

	for {
		select {
		case ack := <-acks:
			select {
			case toBeAcked <- ack:
			case <-ctx.Done():
				return <-errs
			}
			ams.counter.WithLabelValues("success", ams.topic).Inc()
		case <-ctx.Done():
			return <-errs
		case err := <-errs:
			if err != nil {
				ams.counter.WithLabelValues("error", ams.topic).Inc()
			}
			return err
		}
	}
}

// Close closes the message source
func (ams *AsyncMessageSource) Close() error {
	return ams.impl.Close()
}

// Status returns the status of this source, or an error if the status could not be determined.
func (ams *AsyncMessageSource) Status() (*substrate.Status, error) {
	return ams.impl.Status()
}
