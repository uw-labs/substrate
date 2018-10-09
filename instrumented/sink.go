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
	impl    substrate.AsyncMessageSink
	counter *prometheus.CounterVec
	topic   string
}

// NewAsyncMessageSink returns a pointer to a new AsyncMessageSink
func NewAsyncMessageSink(sink substrate.AsyncMessageSink, counterOpts prometheus.CounterOpts, topic string) *AsyncMessageSink {
	counter := prometheus.NewCounterVec(counterOpts, labels)

	if err := prometheus.Register(counter); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			counter = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			panic(err)
		}
	}

	return &AsyncMessageSink{
		impl:    sink,
		counter: counter,
		topic:   topic,
	}
}

// PublishMessages implements message publshing wrapped in instrumentation
func (ams *AsyncMessageSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) (rerr error) {
	successes := make(chan substrate.Message, cap(acks))

	errs := make(chan error)
	go func() {
		defer close(errs)
		errs <- ams.impl.PublishMessages(ctx, successes, messages)
	}()

	for {
		select {
		case success := <-successes:
			ams.counter.WithLabelValues("success", ams.topic).Inc()
			acks <- success
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

// Close closes the message sink
func (ams *AsyncMessageSink) Close() error {
	return ams.impl.Close()
}

// Status returns the status of this sink, or an error if the status could not be determined.
func (ams *AsyncMessageSink) Status() (*substrate.Status, error) {
	return ams.impl.Status()
}
