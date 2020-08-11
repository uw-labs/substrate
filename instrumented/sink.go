package instrumented

import (
	"context"
	"errors"

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
	counter.WithLabelValues("error", topic).Add(0)
	counter.WithLabelValues("success", topic).Add(0)

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
			select {
			case acks <- success:
			case <-ctx.Done():
				return <-errs
			case err := <-errs:
				if isUnexpectedError(err) {
					ams.counter.WithLabelValues("error", ams.topic).Inc()
				}
				return err
			}
		case <-ctx.Done():
			return <-errs
		case err := <-errs:
			if isUnexpectedError(err) {
				ams.counter.WithLabelValues("error", ams.topic).Inc()
			}
			return err
		}
	}
}

func isUnexpectedError(err error) bool {
	if err == nil {
		return false
	}
	// we're expecting producers to mark the stopping of producing by cancelling the context
	if errors.Is(err, context.Canceled) {
		return false
	}

	return true
}

// Close closes the message sink
func (ams *AsyncMessageSink) Close() error {
	return ams.impl.Close()
}

// Status returns the status of this sink, or an error if the status could not be determined.
func (ams *AsyncMessageSink) Status() (*substrate.Status, error) {
	return ams.impl.Status()
}
