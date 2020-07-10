package kafka

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/internal/testshared"
)

func TestAll(t *testing.T) {
	k, err := runServer()
	if err != nil {
		t.Fatal(err)
	}

	defer k.Kill()

	t.Run("Kafka Rebalance", func(t *testing.T) {
		k.testRebalance(t)
	})
	testshared.TestAll(t, k)
}

type testServer struct {
	containerName string
	port          int
}

func (ks *testServer) brokers() []string {
	return []string{fmt.Sprintf("127.0.0.1:%d", ks.port)}
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

	require.Equal(t, context.Canceled, <-c1Err)
	require.Equal(t, context.Canceled, <-c2Err)

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

func (ks *testServer) Kill() error {
	cmd := exec.Command("docker", "rm", "-f", ks.containerName)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error removing container: %s", out)
	}

	return nil
}

func runServer() (*testServer, error) {
	containerName := uuid.New().String()

	cmd := exec.CommandContext(
		context.Background(),
		"docker",
		"run",
		"-d",
		"--rm",
		"--name", containerName,
		"-p", "9092:9092",
		"--env", "ADVERTISED_HOST=127.0.0.1",
		"--env", "ADVERTISED_PORT=9092",
		"uwdev/docker-kafka",
	)
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	port := 0
	// wait for container to start up
loop:
	for {
		portCmd := exec.Command("docker", "port", containerName, "9092/tcp")

		out, err := portCmd.CombinedOutput()
		switch {
		case err == nil:
			outS := string(out) // e.g., 0.0.0.0:32776
			ps := strings.Split(outS, ":")
			if len(ps) != 2 {
				cmd.Process.Kill()
				return nil, fmt.Errorf("docker port returned something strange: %s", outS)
			}
			p, err := strconv.Atoi(strings.TrimSpace(ps[1]))
			if err != nil {
				cmd.Process.Kill()
				return nil, fmt.Errorf("docker port returned something strange: %s", outS)
			}
			port = p
			break loop
		case bytes.Contains(out, []byte("No such container:")):
			// Still starting up. Wait a while.
			time.Sleep(time.Millisecond * 100)
		default:
			return nil, err
		}
	}

	ks := &testServer{containerName, port}

	// wait for cluster to be ready
loop2:
	for {
		config := sarama.NewConfig()
		c, err := sarama.NewConsumer([]string{fmt.Sprintf("localhost:%d", port)}, config)
		if err == nil {
			c.Close()
			break loop2
		}
		time.Sleep(100 * time.Millisecond)
	}

	return ks, nil
}

type message struct {
	data []byte
}

func (msg *message) Data() []byte {
	return msg.data
}

func (msg message) Key() []byte {
	return nil
}
