package kafka

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/uw-labs/substrate"
)

type session struct {
	version       int
	saramaSession sarama.ConsumerGroupSession
}

type consumerGroupHandler struct {
	ctx         context.Context
	toAck       chan<- *consumerMessage
	sessCh      chan<- *session
	rebalanceCh chan<- struct{}
	version     int
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (c *consumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	c.version++
	s := &session{
		version:       c.version,
		saramaSession: sess,
	}
	// send session to the ack processor
	select {
	case <-c.ctx.Done():
	case c.sessCh <- s:
	}
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
func (c *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	// signal to ack processor that rebalance might be happening
	select {
	case <-c.ctx.Done():
	case c.rebalanceCh <- struct{}{}:
	}
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (c *consumerGroupHandler) ConsumeClaim(_ sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// This function can be called concurrently for multiple claims, so the code
	// below, absent locking etc may seem wrong, but it's actually fine.
	// Different partition claims can be processed concurrently, but we funnel
	// them all into c.toAck, which is consumed and processed by a single goroutine.
	for {
		select {
		case <-c.ctx.Done():
			return nil
		case m, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			cm := &consumerMessage{cm: m, sessVersion: c.version}
			select {
			case c.toAck <- cm:
			case <-c.ctx.Done():
				return nil
			}
		}
	}
}

type kafkaAcksProcessor struct {
	toClient    chan<- substrate.Message
	fromKafka   <-chan *consumerMessage
	acks        <-chan substrate.Message
	sessCh      <-chan *session
	rebalanceCh <-chan struct{}

	sess        sarama.ConsumerGroupSession
	sessVersion int
	forAcking   []*consumerMessage
}

func (ap *kafkaAcksProcessor) setSession(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case s := <-ap.sessCh:
		ap.sess = s.saramaSession
		ap.sessVersion = s.version
	}
	return true
}

func (ap *kafkaAcksProcessor) run(ctx context.Context) error {
	// First set session, so that we can acknowledge messages.
	if ok := ap.setSession(ctx); !ok {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ap.rebalanceCh:
			// Wait for the new session.
			if ok := ap.setSession(ctx); !ok {
				return nil
			}
		case msg := <-ap.fromKafka:
			if msg.sessVersion != ap.sessVersion {
				continue // skip message that was written consumed for previous session version
			}
			if err := ap.processMessage(ctx, msg); err != nil {
				if err == context.Canceled {
					// This error is returned when a context cancellation is encountered
					// before the message was sent to the client so, we just return nil,
					// as we do in other context cancellation cases.
					return nil
				}
				return err
			}
		case ack := <-ap.acks:
			if err := ap.processAck(ack); err != nil {
				return err
			}
		}
	}
}

func (ap *kafkaAcksProcessor) processMessage(ctx context.Context, msg *consumerMessage) error {
	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		case <-ap.rebalanceCh:
			// Wait for the new session.
			if ok := ap.setSession(ctx); !ok {
				return context.Canceled
			}
			return nil // We can return immediately as the current message can be discarded.
		case ap.toClient <- msg:
			ap.forAcking = append(ap.forAcking, msg)
			return nil // We have passed the message to the client, so we can exit this loop.
		case ack := <-ap.acks:
			// Still process acks, so that we don't block the consumer acknowledging the message.
			if err := ap.processAck(ack); err != nil {
				return err
			}
		}
	}
}

func (ap *kafkaAcksProcessor) processAck(ack substrate.Message) error {
	switch {
	case len(ap.forAcking) == 0:
		return substrate.InvalidAckError{
			Acked:    ack,
			Expected: nil,
		}
	case ack != ap.forAcking[0]:
		return substrate.InvalidAckError{
			Acked:    ack,
			Expected: ap.forAcking[0],
		}
	case ap.forAcking[0].sessVersion != ap.sessVersion:
		// Discard pending message that was consumed before a rebalance.
		ap.forAcking = ap.forAcking[1:]
	default:
		// Acknowledge the message.
		if ap.forAcking[0].cm != nil {
			ap.sess.MarkMessage(ap.forAcking[0].cm, "")
		} else {
			off := ap.forAcking[0].offset
			// MarkOffset marks the next message to consume, so we need to add 1
			// to the offset to mark this message as consumed. Note that the bsm cluster
			// did this when committing offsets, so that's why it worked without this before.
			ap.sess.MarkOffset(off.topic, off.partition, off.offset+1, "")
		}
		ap.forAcking = ap.forAcking[1:]
	}
	return nil
}
