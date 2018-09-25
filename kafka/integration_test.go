package kafka

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/google/uuid"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/testshared"
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
}

func (ks *testServer) NewConsumer(topic string, groupID string) substrate.AsyncMessageSource {
	return &AsyncMessageSource{
		Brokers:       []string{fmt.Sprintf("localhost:%d", ks.port)},
		ConsumerGroup: groupID,
		Topic:         topic,
		Offset:        OffsetOldest,
	}
}

func (ks *testServer) NewProducer(topic string) substrate.AsyncMessageSink {
	return &AsyncMessageSink{
		Brokers: []string{fmt.Sprintf("localhost:%d", ks.port)},
		Topic:   topic,
	}
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
		"-p", "9092:9092",
		"--env", "ADVERTISED_HOST=127.0.0.1",
		"--env", "ADVERTISED_PORT=9092",
		"spotify/kafka",
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
		config := cluster.NewConfig()
		c, err := cluster.NewConsumer([]string{fmt.Sprintf("localhost:%d", port)}, generateID(), []string{ /* no topics */ }, config)
		if err == nil {
			c.Close()
			break loop2
		}
		time.Sleep(100 * time.Millisecond)
	}

	return ks, nil
}

func generateID() string {
	random := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	_, err := rand.Read(random)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(random)
}
