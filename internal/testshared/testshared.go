package testshared

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uw-labs/substrate"
)

// TestServer represents the server under test, and is expected to be
// implemented by each backend broker implementation wishing to benefit
// from these shared tests.
type TestServer interface {
	NewConsumer(topic string, groupID string) substrate.AsyncMessageSource
	NewProducer(topic string) substrate.AsyncMessageSink
	TestEnd()
}

// TestAll is the main entrypoint from the backend implmenentation tests to
// call, and will run each test as a sub-test.
func TestAll(t *testing.T, ts TestServer) {
	t.Helper()

	for _, x := range []func(t *testing.T, ts TestServer){
		testOnePublisherOneMessageOneConsumer,
		testOnePublisherOneMessageTwoConsumers,
		testPublisherShouldNotBlock,
		testConsumerAckInvalidMessageID,
		testAckWithoutRecieve,
		testConsumeWithoutAck,
		testOnePublisherOneConsumerConsumeWithoutAcking,
		testProduceStatusOk,
		testProduceStatusFail,
		testConsumeStatusOk,
		testConsumeStatusFail,
		testPublishMultipleMessagesOneConsumer,
		testOnePublisherOneConsumerConsumeWithoutAckingDiscardedPayload,
	} {
		f := func(t *testing.T) {
			x(t, ts)
		}
		t.Run(runtime.FuncForPC(reflect.ValueOf(x).Pointer()).Name(), f)
		ts.TestEnd()
	}
}

type testMessage []byte

func (m *testMessage) Data() []byte {
	return []byte(*m)
}

func testOnePublisherOneMessageOneConsumer(t *testing.T, ts TestServer) {
	assert := assert.New(t)

	topic := generateID()
	consumerID := generateID()

	cons := ts.NewConsumer(topic, consumerID)
	prod := ts.NewProducer(topic)

	ctx, cancel := context.WithCancel(context.Background())

	consMsgs := make(chan substrate.Message, 1024)
	consAcks := make(chan substrate.Message, 1024)
	consErrs := make(chan error, 1)
	go func() {
		consErrs <- cons.ConsumeMessages(ctx, consMsgs, consAcks)
		cancel()
	}()

	prodMsgs := make(chan substrate.Message, 1024)
	prodAcks := make(chan substrate.Message, 1024)
	prodErrs := make(chan error, 1)
	go func() {
		prodErrs <- prod.PublishMessages(ctx, prodAcks, prodMsgs)
		cancel()
	}()

	// the message
	messageText := "messageText123"

	m := testMessage([]byte(messageText))
	produceAndCheckAck(ctx, t, prodMsgs, prodAcks, &m)

	msgStr := consumeAndAck(ctx, t, consMsgs, consAcks)
	assert.Equal(messageText, msgStr)

	// we're done
	cancel()
	if err := <-consErrs; err != nil {
		t.Errorf("unexpected error from consume : %s", err)
	}
	if err := <-prodErrs; err != nil {
		t.Errorf("unexpected error from consume : %s", err)
	}

}

func testOnePublisherOneConsumerConsumeWithoutAcking(t *testing.T, ts TestServer) {
	assert := assert.New(t)

	topic := generateID()
	consumerID := generateID()

	cons := ts.NewConsumer(topic, consumerID)
	prod := ts.NewProducer(topic)

	prodCtx, prodCancel := context.WithCancel(context.Background())

	prodMsgs := make(chan substrate.Message, 1024)
	prodAcks := make(chan substrate.Message, 1024)
	prodErrs := make(chan error, 1)
	go func() {
		prodErrs <- prod.PublishMessages(prodCtx, prodAcks, prodMsgs)
	}()

	messageText := "messageText234"

	m := testMessage([]byte(messageText))
	produceAndCheckAck(prodCtx, t, prodMsgs, prodAcks, &m)
	prodCancel()

	if err := <-prodErrs; err != nil {
		t.Errorf("unexpected error from consume : %s", err)
	}
	if err := prod.Close(); err != nil {
		t.Errorf("unexpected error closing producer: %s", err)
	}

	// consume once without ack
	cons1Ctx, cons1Cancel := context.WithCancel(context.Background())
	cons1Msgs := make(chan substrate.Message, 1024)
	cons1Acks := make(chan substrate.Message, 1024)
	cons1Errs := make(chan error, 1)
	go func() {
		cons1Errs <- cons.ConsumeMessages(cons1Ctx, cons1Msgs, cons1Acks)
	}()

	m1 := <-cons1Msgs
	assert.Equal(messageText, string(m1.Data()))
	cons1Cancel()
	if err := <-cons1Errs; err != nil {
		t.Errorf("unexpected error from consume : %s", err)
	}

	// consume again with same id, and we should get the same message
	cons2Ctx, cons2Cancel := context.WithCancel(context.Background())
	cons2Msgs := make(chan substrate.Message, 1024)
	cons2Acks := make(chan substrate.Message, 1024)
	cons2Errs := make(chan error, 1)
	go func() {
		cons2Errs <- cons.ConsumeMessages(cons2Ctx, cons2Msgs, cons2Acks)
	}()

	msgStr := consumeAndAck(cons2Ctx, t, cons2Msgs, cons2Acks)
	assert.Equal(messageText, msgStr)

	cons2Cancel()
	if err := <-cons2Errs; err != nil {
		t.Errorf("unexpected error from consume : %s", err)
	}

	if err := cons.Close(); err != nil {
		t.Errorf("unexpected error closing consumer: %s", err)
	}
}

func testOnePublisherOneMessageTwoConsumers(t *testing.T, ts TestServer) {
	assert := assert.New(t)

	topic := generateID()

	messageText := "messageText345"
	msgID := generateID()

	connectSendmessageAndClose(t, ts, topic, messageText, msgID)

	ctx, cancel := context.WithCancel(context.Background())

	cons1Msgs := make(chan substrate.Message, 1024)
	cons1Acks := make(chan substrate.Message, 1024)
	cons1Errs := make(chan error, 1)
	go func() {
		cons := ts.NewConsumer(topic, generateID())
		cons1Errs <- cons.ConsumeMessages(ctx, cons1Msgs, cons1Acks)
	}()

	cons2Msgs := make(chan substrate.Message, 1024)
	cons2Acks := make(chan substrate.Message, 1024)
	cons2Errs := make(chan error, 1)
	go func() {
		cons := ts.NewConsumer(topic, generateID())
		cons2Errs <- cons.ConsumeMessages(ctx, cons2Msgs, cons2Acks)
	}()

	// consumer 1
	msgStr1 := consumeAndAck(ctx, t, cons1Msgs, cons1Acks)
	assert.Equal(messageText, msgStr1)

	// consumer 2
	msgStr2 := consumeAndAck(ctx, t, cons2Msgs, cons2Acks)
	assert.Equal(messageText, msgStr2)

	// we're done
	cancel()
	if err := <-cons1Errs; err != nil {
		t.Errorf("unexpected error from consume : %s", err)
	}

}

func testPublisherShouldNotBlock(t *testing.T, ts TestServer) {
	topic := generateID()
	var testMessages []*testMessage
	for i := 0; i < 100000; i++ {
		m := testMessage([]byte(fmt.Sprintf("message%v", i)))
		testMessages = append(testMessages, &m)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	prod := ts.NewProducer(topic)

	prodMsgs := make(chan substrate.Message, 1024)
	prodAcks := make(chan substrate.Message, 1024)
	prodErrs := make(chan error, 2)
	go func() {
		prodErrs <- prod.PublishMessages(ctx, prodAcks, prodMsgs)
	}()

	for _, m := range testMessages {
		select {
		case prodMsgs <- m:
		case <-prodAcks:
		case err := <-prodErrs:
			if err != nil {
				t.Errorf("unexpected error from consume : %s", err)
			}
		case <-ctx.Done():
			t.Fatal("Producer should not block")
		}
	}
}

func testConsumerAckInvalidMessageID(t *testing.T, ts TestServer) {
	assert := assert.New(t)

	topic := generateID()

	messageText := "messageText456"

	connectSendmessageAndClose(t, ts, topic, messageText, generateID())

	ctx := context.Background()

	consMsgs := make(chan substrate.Message, 1024)
	consAcks := make(chan substrate.Message, 1024)
	consErrs := make(chan error, 1)
	go func() {
		cons := ts.NewConsumer(topic, generateID())
		consErrs <- cons.ConsumeMessages(ctx, consMsgs, consAcks)
	}()

	// wait for message
	var m substrate.Message

	select {
	case m = <-consMsgs:
	case <-ctx.Done():
		t.Error(ctx.Err())
	}

	// send invalid ack
	anotherMessage := &testMessage{}
	select {
	case consAcks <- anotherMessage:
	case <-ctx.Done():
		t.Error(ctx.Err())
	}

	// expect error
	select {
	case err := <-consErrs:
		assert.Equal(substrate.InvalidAckError{
			Acked:    anotherMessage,
			Expected: m,
		}, err)
		//		assert.EqualError(err, "asdasd")
	case <-ctx.Done():
		t.Error(ctx.Err())
	}
}

func testAckWithoutRecieve(t *testing.T, ts TestServer) {
	assert := assert.New(t)

	topic := generateID()

	ctx := context.Background()

	consMsgs := make(chan substrate.Message, 1024)
	consAcks := make(chan substrate.Message, 1024)
	consErrs := make(chan error, 1)
	go func() {
		cons := ts.NewConsumer(topic, generateID())
		consErrs <- cons.ConsumeMessages(ctx, consMsgs, consAcks)
	}()

	// send invalid ack
	anotherMessage := &testMessage{}
	select {
	case consAcks <- anotherMessage:
	case <-ctx.Done():
		t.Error(ctx.Err())
	}

	// expect error
	select {
	case err := <-consErrs:
		assert.Equal(substrate.InvalidAckError{
			Acked:    anotherMessage,
			Expected: nil,
		}, err)
	case <-ctx.Done():
		t.Error(ctx.Err())
	}
}

func testConsumeWithoutAck(t *testing.T, ts TestServer) {
	assert := assert.New(t)

	topic := generateID()

	messageText := "messageText567"
	connectSendmessageAndClose(t, ts, topic, messageText, generateID())

	ctx, cancel := context.WithCancel(context.Background())

	consumerID := generateID()

	cons1Msgs := make(chan substrate.Message, 1024)
	cons1Acks := make(chan substrate.Message, 1024)
	cons1Errs := make(chan error, 1)
	go func() {
		cons := ts.NewConsumer(topic, consumerID)
		cons1Errs <- cons.ConsumeMessages(ctx, cons1Msgs, cons1Acks)
	}()

	// get message but don't ack it
	select {
	case m := <-cons1Msgs:
		assert.Equal(messageText, string(m.Data()))
	case <-ctx.Done():
		t.Error("canceled early")
	}

	// close connection
	cancel()
	select {
	case <-cons1Msgs:
	case err := <-cons1Errs:
		if err != nil {
			t.Error(err) //TODO: prolly not.
		}
	}

	ctx, cancel = context.WithCancel(context.Background())

	cons2Msgs := make(chan substrate.Message, 1024)
	cons2Acks := make(chan substrate.Message, 1024)
	cons2Errs := make(chan error, 1)
	go func() {
		cons := ts.NewConsumer(topic, consumerID)
		cons2Errs <- cons.ConsumeMessages(ctx, cons2Msgs, cons2Acks)
	}()

	// message from consumer 2
	msgStr2 := consumeAndAck(ctx, t, cons2Msgs, cons2Acks)
	assert.Equal(messageText, msgStr2)

	// we're done
	cancel()
	if err := <-cons2Errs; err != nil {
		t.Errorf("unexpected error from consume : %s", err)
	}

}

func testProduceStatusOk(t *testing.T, ts TestServer) {
	assert := assert.New(t)

	// very basic happy path test.
	prod := ts.NewProducer(generateID())
	status, err := prod.Status()
	assert.NoError(err)

	assert.Equal(&substrate.Status{Working: true}, status)
}

func testProduceStatusFail(t *testing.T, ts TestServer) {
	t.Skip("TODO: this needs a test, but the current TestServer abstraction doesn't led itself to this yet")
}

func testConsumeStatusOk(t *testing.T, ts TestServer) {
	assert := assert.New(t)

	// very basic happy path test.
	cons := ts.NewConsumer(generateID(), generateID())
	status, err := cons.Status()
	assert.NoError(err)

	assert.Equal(&substrate.Status{Working: true}, status)
}

func testConsumeStatusFail(t *testing.T, ts TestServer) {
	t.Skip("TODO: this needs a test, but the current TestServer abstraction doesn't led itself to this yet")
}

func testPublishMultipleMessagesOneConsumer(t *testing.T, ts TestServer) {
	assert := assert.New(t)

	topic := generateID()
	consumerID := generateID()

	cons := ts.NewConsumer(topic, consumerID)
	prod := ts.NewProducer(topic)

	ctx, cancel := context.WithCancel(context.Background())

	consMsgs := make(chan substrate.Message, 1024)
	consAcks := make(chan substrate.Message, 1024)
	consErrs := make(chan error, 1)
	go func() {
		consErrs <- cons.ConsumeMessages(ctx, consMsgs, consAcks)
		cancel()
	}()

	prodMsgs := make(chan substrate.Message, 1024)
	prodAcks := make(chan substrate.Message, 1024)
	prodErrs := make(chan error, 1)
	go func() {
		prodErrs <- prod.PublishMessages(ctx, prodAcks, prodMsgs)
		cancel()
	}()

	for i := 0; i < 30; i++ {
		messageText := fmt.Sprintf("messageText-%d", i)
		m := testMessage([]byte(messageText))
		produceAndCheckAck(ctx, t, prodMsgs, prodAcks, &m)
	}

	for i := 0; i < 30; i++ {
		messageText := fmt.Sprintf("messageText-%d", i)
		msgStr := consumeAndAck(ctx, t, consMsgs, consAcks)
		assert.Equal(messageText, msgStr)
	}

	// we're done
	cancel()
	if err := <-consErrs; err != nil {
		t.Errorf("unexpected error from consume : %s", err)
	}
	if err := <-prodErrs; err != nil {
		t.Errorf("unexpected error from consume : %s", err)
	}

}

func testOnePublisherOneConsumerConsumeWithoutAckingDiscardedPayload(t *testing.T, ts TestServer) {
	assert := assert.New(t)

	topic := generateID()
	consumerID := generateID()

	cons := ts.NewConsumer(topic, consumerID)
	prod := ts.NewProducer(topic)

	prodCtx, prodCancel := context.WithCancel(context.Background())

	prodMsgs := make(chan substrate.Message, 1024)
	prodAcks := make(chan substrate.Message, 1024)
	prodErrs := make(chan error, 1)
	go func() {
		prodErrs <- prod.PublishMessages(prodCtx, prodAcks, prodMsgs)
	}()

	messageText := "messageText234"

	m := testMessage([]byte(messageText))
	produceAndCheckAck(prodCtx, t, prodMsgs, prodAcks, &m)
	prodCancel()

	if err := <-prodErrs; err != nil {
		t.Errorf("unexpected error from consume : %s", err)
	}
	if err := prod.Close(); err != nil {
		t.Errorf("unexpected error closing producer: %s", err)
	}

	// consume once without ack
	cons1Ctx, cons1Cancel := context.WithCancel(context.Background())
	cons1Msgs := make(chan substrate.Message, 1024)
	cons1Acks := make(chan substrate.Message, 1024)
	cons1Errs := make(chan error, 1)
	go func() {
		cons1Errs <- cons.ConsumeMessages(cons1Ctx, cons1Msgs, cons1Acks)
	}()

	m1 := <-cons1Msgs
	assert.Equal(messageText, string(m1.Data()))
	cons1Cancel()
	if err := <-cons1Errs; err != nil {
		t.Errorf("unexpected error from consume : %s", err)
	}

	// consume again with same id, and we should get the same message
	cons2Ctx, cons2Cancel := context.WithCancel(context.Background())
	cons2Msgs := make(chan substrate.Message, 1024)
	cons2Acks := make(chan substrate.Message, 1024)
	cons2Errs := make(chan error, 1)
	go func() {
		cons2Errs <- cons.ConsumeMessages(cons2Ctx, cons2Msgs, cons2Acks)
	}()

	msgStr := consumeAndAckDiscard(cons2Ctx, t, cons2Msgs, cons2Acks)
	assert.Equal(messageText, msgStr)

	cons2Cancel()
	if err := <-cons2Errs; err != nil {
		t.Errorf("unexpected error from consume : %s", err)
	}

	if err := cons.Close(); err != nil {
		t.Errorf("unexpected error closing consumer: %s", err)
	}
}

// Helper functions below here

func connectSendmessageAndClose(t *testing.T, ts TestServer, topic string, messageText string, msgID string) {
	ctx, cancel := context.WithCancel(context.Background())
	prod := ts.NewProducer(topic)

	prodMsgs := make(chan substrate.Message, 1)
	prodAcks := make(chan substrate.Message, 1)
	prodErrs := make(chan error, 1)
	go func() {
		prodErrs <- prod.PublishMessages(ctx, prodAcks, prodMsgs)
	}()
	message := testMessage([]byte(messageText))
	produceAndCheckAck(ctx, t, prodMsgs, prodAcks, &message)
	cancel()
	if err := <-prodErrs; err != nil {
		t.Errorf("unexpected error from consume : %s", err)
	}
	if err := prod.Close(); err != nil {
		t.Errorf("unexpected error closing producer: %s", err)
	}
}

func produceAndCheckAck(ctx context.Context, t *testing.T, prodMsgs chan<- substrate.Message, prodAcks <-chan substrate.Message, m substrate.Message) {
	t.Helper()
	// send it
	select {
	case prodMsgs <- m:
	case <-ctx.Done():
		if ctx.Err() != nil {
			t.Error(ctx.Err())
		}
	}
	// get ack
	select {
	case prodAck := <-prodAcks:
		assert.Equal(t, prodAck, m)
	case <-ctx.Done():
		if ctx.Err() != nil {
			t.Error(ctx.Err())
		}
	}
}

func consumeAndAck(ctx context.Context, t *testing.T, consMsgs <-chan substrate.Message, consAcks chan<- substrate.Message) string {

	var msg substrate.Message

	// receive it
	select {
	case msg = <-consMsgs:
	case <-ctx.Done():
		if ctx.Err() != nil {
			t.Error(ctx.Err())
		}
		return ""
	}

	// ack receipt
	select {
	case consAcks <- msg:
	case <-ctx.Done():
		if ctx.Err() != nil {
			t.Error(ctx.Err())
		}
		return ""
	}

	return string(msg.Data())
}

func consumeAndAckDiscard(ctx context.Context, t *testing.T, consMsgs <-chan substrate.Message, consAcks chan<- substrate.Message) string {

	var msg substrate.Message

	// receive it
	select {
	case msg = <-consMsgs:
	case <-ctx.Done():
		if ctx.Err() != nil {
			t.Error(ctx.Err())
		}
		return ""
	}

	ret := string(msg.Data())

	if dm, ok := msg.(substrate.DiscardableMessage); ok {
		dm.DiscardPayload()
	}

	// ack receipt
	select {
	case consAcks <- msg:
	case <-ctx.Done():
		if ctx.Err() != nil {
			t.Error(ctx.Err())
		}
		return ""
	}

	return ret
}

func generateID() string {
	random := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	_, err := rand.Read(random)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(random)
}
