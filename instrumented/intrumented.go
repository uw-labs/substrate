package instrumented

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/uw-labs/substrate"
)

var labels = []string{"status", "topic"}

// AsyncMessageSink is an instrumented message sink
// The counter vector will have the labels "status" and "topic"
type AsyncMessageSink struct {
	impl       substrate.AsyncMessageSink
	counter    *prometheus.CounterVec
	topic      string
	bufferSize int
}

// NewAsyncMessageSink returns a pointer to a new AsyncMessageSink
func NewAsyncMessageSink(sink substrate.AsyncMessageSink, counterOpts prometheus.CounterOpts, topic string, bufferSize int) *AsyncMessageSink {
	counter := prometheus.NewCounterVec(counterOpts, labels)

	if err := prometheus.Register(counter); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			counter = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			panic(err)
		}
	}

	return &AsyncMessageSink{
		impl:       sink,
		counter:    counter,
		topic:      topic,
		bufferSize: bufferSize,
	}
}

// PublishMessages implements message publshing wrapped in instrumentation
func (ams *AsyncMessageSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) (rerr error) {
	successes := make(chan substrate.Message, ams.bufferSize)

	errs := make(chan error)
	go func() {
		defer close(errs)
		errs <- ams.impl.PublishMessages(ctx, successes, messages)
	}()

	for {
		select {
		case success := <-successes:
			acks <- success
			ams.counter.WithLabelValues("success", ams.topic).Inc()
		case <-ctx.Done():
			return nil
		case err := <-errs:
			if err != nil {
				ams.counter.WithLabelValues("error", ams.topic).Inc()
				return err
			}
			return nil
		}
	}
}

// Close closes the message sink
func (ams *AsyncMessageSink) Close() error {
	return ams.impl.Close()
}

// Status returns the status of this sink, or an error if the status could not be determined.
func (ams *AsyncMessageSink) Status() (*substrate.Status, error) {
	return ams.impl.Status()
}

// AsyncMessageSource is an instrumented message source
// The counter vector will have the labels "status" and "topic"
type AsyncMessageSource struct {
	impl       substrate.AsyncMessageSource
	counter    *prometheus.CounterVec
	topic      string
	bufferSize int
}

// NewAsyncMessageSource returns a pointer to a new AsyncMessageSource
func NewAsyncMessageSource(source substrate.AsyncMessageSource, counterOpts prometheus.CounterOpts, topic string, bufferSize int) *AsyncMessageSource {
	counter := prometheus.NewCounterVec(counterOpts, labels)

	if err := prometheus.Register(counter); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			counter = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			panic(err)
		}
	}

	return &AsyncMessageSource{
		impl:       source,
		counter:    counter,
		topic:      topic,
		bufferSize: bufferSize,
	}
}

// ConsumeMessages implements message consuming wrapped in instrumentation
func (ams *AsyncMessageSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	toBeAcked := make(chan substrate.Message, ams.bufferSize)

	errs := make(chan error)
	go func() {
		defer close(errs)
		errs <- ams.impl.ConsumeMessages(ctx, messages, toBeAcked)
	}()

	for {
		select {
		case ack := <-acks:
			toBeAcked <- ack
			ams.counter.WithLabelValues("success", ams.topic).Inc()
		case <-ctx.Done():
			return nil
		case err := <-errs:
			if err != nil {
				ams.counter.WithLabelValues("error", ams.topic).Inc()
				return err
			}
			return nil
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
