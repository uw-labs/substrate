package liftbridge

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
	lift "github.com/liftbridge-io/go-liftbridge"
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
	client        lift.Client
}

func (s *testServer) NewConsumer(topic string, _ string) substrate.AsyncMessageSource {
	if err := s.client.CreateStream(context.Background(), topic, topic); err != nil {
		if err != lift.ErrStreamExists {
			panic(err)
		}
	}
	source, err := NewAsyncMessageSource(AsyncMessageSourceConfig{
		Brokers: []string{"localhost:9292"},
		Topic:   topic,
	})
	if err != nil {
		panic(err)
	}
	return source
}

func (s *testServer) NewProducer(topic string) substrate.AsyncMessageSink {
	if err := s.client.CreateStream(context.Background(), topic, topic); err != nil {
		if err != lift.ErrStreamExists {
			panic(err)
		}
	}
	sink, err := NewAsyncMessageSink(AsyncMessageSinkConfig{
		Brokers: []string{"localhost:9292"},
		Topic:   topic,
	})
	if err != nil {
		panic(err)
	}
	return sink
}

func (s *testServer) TestEnd() {}

func (s *testServer) Kill() error {
	if err := s.client.Close(); err != nil {
		return err
	}

	cmd := exec.Command("docker", "rm", "-f", s.containerName)

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
		"-p", "9292:9292",
		"-p", "8222:8222",
		"-p", "6222:6222",
		"liftbridge/standalone-dev",
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

	s := &testServer{
		containerName: containerName,
		port:          port,
		clusterID:     "test-cluster",
	}

loop2:
	for {
		client, err := lift.Connect([]string{"localhost:9292"})
		if err == nil {
			s.client = client
			if err := s.client.CreateStream(context.Background(), "liftbridge-test-topic", "liftbridge-test-topic"); err != nil {
				if err != lift.ErrStreamExists {
					_ = s.client.Close()
					continue
				}
			}
			break loop2
		}
		time.Sleep(100 * time.Millisecond)
	}

	return s, nil
}
