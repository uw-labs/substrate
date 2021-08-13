package jetstream

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/internal/testshared"
)

func TestAll(t *testing.T) {
	k, err := runServer()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		k.Kill()
	})

	testshared.TestAll(t, &testServer{}, true)
}

type testServer struct {
	containerName string
}

func (ks *testServer) NewConsumer(topic string, groupID string) substrate.AsyncMessageSource {
	ks.ensureTopic(topic)
	source, err := NewAsyncMessageSource(AsyncMessageSourceConfig{
		URL:           "http://0.0.0.0:4222",
		Topic:         topic,
		ConsumerGroup: groupID,
		Offset:        OffsetOldest,
		AckWait:       time.Second,
	})
	if err != nil {
		panic(err)
	}
	return source
}

func (ks *testServer) NewProducer(topic string) substrate.AsyncMessageSink {
	ks.ensureTopic(topic)
	conf := AsyncMessageSinkConfig{
		URL:   "http://0.0.0.0:4222",
		Topic: topic,
	}

	sink, err := NewAsyncMessageSink(conf)
	if err != nil {
		panic(err)
	}
	return sink
}

func (ks *testServer) ensureTopic(topic string) {
	conn, err := nats.Connect("http://0.0.0.0:4222")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	js, err := conn.JetStream()
	if err != nil {
		panic(err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name: topic,
	}); err != nil {
		panic(err)
	}
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
		"-p", "4222:4222",
		"nats",
		"-js",
	)
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	// wait for container to start up
loop:
	for {
		portCmd := exec.Command("docker", "port", containerName, "4222/tcp")

		out, err := portCmd.CombinedOutput()
		switch {
		case err == nil:
			break loop
		case bytes.Contains(out, []byte("No such container:")):
			// Still starting up. Wait a while.
			time.Sleep(time.Millisecond * 100)
		default:
			return nil, err
		}
	}

	ks := &testServer{containerName}
	for {
		if err := ks.canConsume(); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return ks, nil
}

func (ks *testServer) canConsume() error {
	conn, err := nats.Connect("http://0.0.0.0:4222")
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}
