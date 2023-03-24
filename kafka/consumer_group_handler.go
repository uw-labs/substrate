package kafka

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/internal/debug"
)

type consumerGroupHandler struct {
	ctx         context.Context
	toAck       chan<- *consumerMessage
	sessCh      chan<- sarama.ConsumerGroupSession
	rebalanceCh chan<- struct{}

	debugger debug.Debugger
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (c *consumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	// send session to the ack processor
	select {
	case <-c.ctx.Done():
	case c.sessCh <- sess:
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
func (c *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
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
			cm := &consumerMessage{cm: m}
			select {
			case c.toAck <- cm:
			case <-c.ctx.Done():
				return nil
			}
		case <-sess.Context().Done():
			return nil
		}
	}
}

type kafkaAcksProcessor struct {
	toClient    chan<- substrate.Message
	fromKafka   <-chan *consumerMessage
	acks        <-chan substrate.Message
	sessCh      <-chan sarama.ConsumerGroupSession
	rebalanceCh <-chan struct{}

	sess      sarama.ConsumerGroupSession
	forAcking []*consumerMessage

	debugger debug.Debugger
}

func (ap *kafkaAcksProcessor) run(ctx context.Context) error {
	// First set session, so that we can acknowledge messages.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ap.sess = <-ap.sessCh:
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ap.rebalanceCh:
			// Mark all pending messages to be discarded, as rebalance happened.
			for _, msg := range ap.forAcking {
				msg.discard = true
			}
			// Wait for the new session.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ap.sess = <-ap.sessCh:
			}
		case msg := <-ap.fromKafka:
			ap.debugger.Logf("substrate : consumer - got message from kafka : %s\n", msg)
			if err := ap.processMessage(ctx, msg); err != nil {
				return err
			}
		case ack := <-ap.acks:
			ap.debugger.Logf("substrate : consumer - got ack from caller for message : %s\n", ack)
			if err := ap.processAck(ack); err != nil {
				return err
			}
		}
	}
}

func (ap *kafkaAcksProcessor) processMessage(ctx context.Context, msg *consumerMessage) error {
	var pl []byte
	if ap.debugger.Enabled {
		// grab the data now, because it may be discarded later.
		pl = msg.Data()
	}
	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		case <-ap.rebalanceCh:
			// Mark all pending messages to be discarded, as rebalance happened.
			for _, msg := range ap.forAcking {
				msg.discard = true
			}
			// Wait for the new session.
			select {
			case <-ctx.Done():
			case ap.sess = <-ap.sessCh:
			}
			return nil // We can return immediately as the current message can be discarded.
		case ap.toClient <- msg:
			ap.debugger.Logf("substrate : consumer - sent message to caller : %s\n", pl)
			ap.forAcking = append(ap.forAcking, msg)
			return nil // We have passed the message to the client, so we can exit this loop.
		case ack := <-ap.acks:
			ap.debugger.Logf("substrate : consumer - got ack from caller for message : %s\n", msg)
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
	case ap.forAcking[0].discard:
		// Discard pending message that was consumed before a rebalance.
		ap.forAcking = ap.forAcking[1:]
	default:
		// Acknowledge the message.
		if ap.forAcking[0].cm != nil {
			ap.sess.MarkMessage(ap.forAcking[0].cm, "")
			ap.debugger.Logf("substrate : consumer - sent ack to kafka for message : %s\n", ap.forAcking[0])
		} else {
			off := ap.forAcking[0].offset
			// MarkOffset marks the next message to consume, so we need to add 1
			// to the offset to mark this message as consumed. Note that the bsm cluster
			// did this when committing offsets, so that's why it worked without this before.
			ap.sess.MarkOffset(off.topic, off.partition, off.offset+1, "")
			ap.debugger.Logf("substrate : consumer - sent ack to kafka for message : [payload not available]\n")
		}
		ap.forAcking = ap.forAcking[1:]
	}
	return nil
}
