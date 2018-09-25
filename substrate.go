package substrate

import (
	"context"
	"fmt"
)

// Message is the single type that represents all messages in substrate.
type Message interface {
	Data() []byte
}

// AsyncMessageSink represents a message sink that allows publishing messages,
// and multiple messages can be in flight before any acks are recieved,
// depending upon the configuration of the underlying message sink.
type AsyncMessageSink interface {
	// PublishMessages publishes any messages found on the `messages`
	// channel and returns them on the `acks` channel once they have
	// been published.  This function will block until `ctx` is done,
	// or until an error occurs.  Messages will always be processed
	// and acknowledged in order.
	PublishMessages(ctx context.Context, acks chan<- Message, messages <-chan Message) error
	// Close permanently closes the AsyncMessageSink and frees underlying resources
	Close() error
	Statuser
}

// AsyncMessageSource represents a message source that allows consuming
// messages, and multiple messages can be in flight before any acks are sent,
// depending upon the configuration of the underlying message source.
type AsyncMessageSource interface {
	// ConsumeMessages provides messages to the caller on the `messages`
	// channel and expects them to be sent back to the `acks` channel once
	// that have been handled properly.  This function will block until
	// `ctx` is done, or until an error occurs.
	ConsumeMessages(ctx context.Context, messages chan<- Message, acks <-chan Message) error
	// Close permanently closes the AsyncMessageSource and frees underlying resources
	Close() error
	Statuser
}

// ConsumerMessageHandler is the callback function type that synchronous
// message consumers must implement.
type ConsumerMessageHandler func(Message) error

// SynchronousMessageSource represents a message source that allows "message
// at a time" consumption and relieves the consumer from having to deal with
// acknowledgements themselves.
type SynchronousMessageSource interface {
	// Close closed the SynchronousMessageSource, freeing underlying
	// resources.
	Close() error
	// ConsumeMessages calls the `handler` function for each messages
	// available to consume.  If the handler returns no error, an
	// acknowledgement will be sent to the broker.  If an error is returned
	// by the handler, it will be propogated and returned from this
	// function.  This function will block until `ctx` is done or until an
	// error occurs.
	ConsumeMessages(ctx context.Context, handler ConsumerMessageHandler) error
	Statuser
}

// SynchronousMessageSink represents a message source that allows "message at
// a time" publishing and relieves the consumer from having to deal with
// acknowledgements themselves.
type SynchronousMessageSink interface {
	// Close closed the SynchronousMessageSink, freeing underlying
	// resources.
	Close() error
	// PublishMessage publishes messages to the broker, waiting for
	// confirmation from the broker before returning.
	PublishMessage(context.Context, Message) error
	Statuser
}

// InvalidAckError means that a message acknowledgement was not as expected.
// This is possilbly from mis-use of the asynchronous APIs, for example acking
// out of order.
type InvalidAckError struct {
	Acked    Message
	Expected Message
}

func (e InvalidAckError) Error() string {
	return fmt.Sprintf("Message ack was out of order. Expected message '%s' but got '%s'", e.Expected, e.Acked)
}

// Statuser is the interface that wraps the Status method.
type Statuser interface {
	Status() (*Status, error)
}

// Status represents a snapshot of the state of a source or sink.
type Status struct {
	// Working indicates whether the source or sink is in a working state
	Working bool
	// Problems indicates and problems with the source or sink, whether or not they prevent it working.
	Problems []string
}
