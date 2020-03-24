package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
	"github.com/uw-labs/podrick"
	_ "github.com/uw-labs/podrick/runtimes/docker"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/internal/testshared"
)

func TestAll(t *testing.T) {
	ctr, err := podrick.StartContainer(context.Background(), "uwdev/docker-kafka", "latest", "9092",
		podrick.WithEnv([]string{
			"ADVERTISED_HOST=127.0.0.1",
			"ADVERTISED_PORT=9092",
		}),
		podrick.WithLivenessCheck(func(address string) error {
			fmt.Println("liveness check", address)
			c, err := sarama.NewClient([]string{address}, sarama.NewConfig())
			if err != nil {
				fmt.Println(err)
				return err
			}
			return c.Close()
		}),
	)
	require.NoError(t, err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		require.NoError(t, ctr.Close(ctx))
	}()

	k := &testServer{
		address: ctr.Address(),
	}
	t.Run("Kafka Rebalance", func(t *testing.T) {
		k.testRebalance(t)
	})
	testshared.TestAll(t, k)
}

type testServer struct {
	address string
}

func (ks *testServer) brokers() []string {
	fmt.Println(ks.address)
	return []string{ks.address}
}

func (ks *testServer) testRebalance(t *testing.T) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_4_0_0

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()
	admin, err := sarama.NewClusterAdmin(ks.brokers(), cfg)
	require.NoError(t, err)

	topic := "rebalancing-test"
	consumerGroup := "group-consumers"
	require.NoError(t, admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     6,
		ReplicationFactor: 1,
	}, false))

	var expectedMsgs []string
	c1Err, c2Err := make(chan error, 1), make(chan error, 1)
	c1Msgs, c2Msgs := map[string]bool{}, map[string]bool{}

	c1Ctx, c1Cancel := context.WithCancel(ctx)
	go func() {
		c1 := substrate.NewSynchronousMessageSource(ks.NewConsumer(topic, consumerGroup))
		defer func() { require.NoError(t, c1.Close()) }()

		c1Err <- c1.ConsumeMessages(c1Ctx, func(_ context.Context, msg substrate.Message) error {
			c1Msgs[string(msg.Data())] = true
			return nil
		})
	}()

	p := substrate.NewSynchronousMessageSink(ks.NewProducer(topic))
	defer func() { require.NoError(t, p.Close()) }()
	for i := 0; i < 5; i++ {
		payload := fmt.Sprintf("message-%v", i)
		expectedMsgs = append(expectedMsgs, payload)
		require.NoError(t, p.PublishMessage(ctx, &message{data: []byte(payload)}))
	}
	time.Sleep(time.Second * 5) // Sleep to read some messages.

	c2Ctx, c2Cancel := context.WithCancel(ctx)
	go func() {
		c2 := substrate.NewSynchronousMessageSource(ks.NewConsumer(topic, consumerGroup))
		defer func() { require.NoError(t, c2.Close()) }()

		c2Err <- c2.ConsumeMessages(c2Ctx, func(_ context.Context, msg substrate.Message) error {
			c2Msgs[string(msg.Data())] = true
			return nil
		})
	}()
	time.Sleep(time.Second * 20) // Sleep to wait for rebalance.

	for i := 5; i < 10; i++ {
		payload := fmt.Sprintf("message-%v", i)
		expectedMsgs = append(expectedMsgs, payload)
		require.NoError(t, p.PublishMessage(ctx, &message{data: []byte(payload)}))
	}
	time.Sleep(time.Second * 5) // Sleep to read some messages.
	c1Cancel()
	time.Sleep(time.Second * 20) // Sleep to wait for rebalance.

	for i := 10; i < 15; i++ {
		payload := fmt.Sprintf("message-%v", i)
		expectedMsgs = append(expectedMsgs, payload)
		require.NoError(t, p.PublishMessage(ctx, &message{data: []byte(payload)}))
	}
	time.Sleep(time.Second * 5) // Sleep to read all remaining messages.
	c2Cancel()

	require.NoError(t, <-c1Err)
	require.NoError(t, <-c2Err)

	var actualMsgs []string
	for msg := range c1Msgs {
		actualMsgs = append(actualMsgs, msg)
	}
	for msg := range c2Msgs {
		if !c1Msgs[msg] {
			actualMsgs = append(actualMsgs, msg)
		}
	}
	require.ElementsMatch(t, expectedMsgs, actualMsgs)
}

func (ks *testServer) NewConsumer(topic string, groupID string) substrate.AsyncMessageSource {
	s, err := NewAsyncMessageSource(AsyncMessageSourceConfig{
		Brokers:       ks.brokers(),
		ConsumerGroup: groupID,
		Topic:         topic,
		Offset:        OffsetOldest,
		Version:       "2.4.0",
	})

	if err != nil {
		panic(err)
	}
	return s
}

func (ks *testServer) NewProducer(topic string) substrate.AsyncMessageSink {
	s, err := NewAsyncMessageSink(AsyncMessageSinkConfig{
		Brokers: ks.brokers(),
		Topic:   topic,
	})
	if err != nil {
		panic(err)
	}
	return s
}

func (ks *testServer) TestEnd() {}

type message struct {
	data []byte
}

func (msg *message) Data() []byte {
	return msg.data
}
