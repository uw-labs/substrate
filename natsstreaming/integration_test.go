package natsstreaming

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/internal/testshared"
)

func TestAll(t *testing.T) {

	k, err := runServer()

	if err != nil {
		t.Fatal(err)
	}

	defer k.Kill()

	testshared.TestAll(t, k)
}

type testServer struct {
	containerName string
	port          int
	clusterID     string
}

func (ks *testServer) NewConsumer(topic string, groupID string) substrate.AsyncMessageSource {
	source, err := NewAsyncMessageSource(AsyncMessageSourceConfig{
		URL:       "http://localhost:4222",
		ClusterID: ks.clusterID,

		QueueGroup: groupID,
		Subject:    topic,
	})
	if err != nil {
		panic(err)
	}
	return source
}

func (ks *testServer) NewProducer(topic string) substrate.AsyncMessageSink {
	conf := AsyncMessageSinkConfig{
		ClientID:  generateID(),
		ClusterID: ks.clusterID,
		Subject:   topic,
		URL:       "http://localhost:4222",
	}

	sink, err := NewAsyncMessageSink(conf)
	if err != nil {
		panic(err)
	}
	return sink
}

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
		"nats-streaming:0.11.2-linux",
	)
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	port := 0
	// wait for container to start up
loop:
	for {
		portCmd := exec.Command("docker", "port", containerName, "4222/tcp")

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

	ks := &testServer{containerName, port, "test-cluster"}

	// wait for cluster to be ready
	cancelled, canc := context.WithCancel(context.Background())
	canc()
loop2:
	for {
		err := ks.canConsume("topic", "group")
		//
		if err == nil {
			break loop2
		}
		if err == cancelled.Err() {
			break loop2
		}
		time.Sleep(100 * time.Millisecond)
	}
loop3:
	for {
		err := ks.NewConsumer("topic", "group").ConsumeMessages(cancelled, nil, nil)
		//
		if err == nil {
			break loop3
			//ks.Kill()
			//return nil, errors.New("what?")
		}
		if err == cancelled.Err() {
			break loop3
		}
		time.Sleep(100 * time.Millisecond)
	}

	return ks, nil
}

func (ks *testServer) canConsume(topic string, groupID string) error {
	source, err := NewAsyncMessageSource(AsyncMessageSourceConfig{
		URL:       "http://localhost:4222",
		ClusterID: ks.clusterID,

		QueueGroup: groupID,
		Subject:    topic,
	})
	if err != nil {
		return err
	}
	return source.Close()
}
