package proximo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"google.golang.org/grpc"

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
	cmd  *exec.Cmd
	port int
}

func (ts *testServer) NewConsumer(topic string, groupID string) substrate.AsyncMessageSource {
	s, err := NewAsyncMessageSource(AsyncMessageSourceConfig{
		Broker:        fmt.Sprintf("localhost:%d", ts.port),
		ConsumerGroup: groupID,
		Topic:         topic,
		Offset:        OffsetOldest,
		Insecure:      true,
	})
	if err != nil {
		panic(err)
	}
	return s
}

func (ts *testServer) NewProducer(topic string) substrate.AsyncMessageSink {
	s, err := NewAsyncMessageSink(AsyncMessageSinkConfig{
		Broker:   fmt.Sprintf("localhost:%d", ts.port),
		Topic:    topic,
		Insecure: true,
		Debug:    true,
	})
	if err != nil {
		panic(err)
	}
	return s
}

func (ts *testServer) TestEnd() {}

func (ts *testServer) Kill() error {
	return ts.cmd.Process.Kill()
}

func runServer() (*testServer, error) {
	cmd := exec.CommandContext(
		context.Background(),
		"proximo-server",
		"mem",
	)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	ts := &testServer{cmd, 6868}

	// Wait for server to start
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, fmt.Sprintf("localhost:%d", ts.port), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return ts, nil
}

func generateID() string {
	random := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	_, err := rand.Read(random)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(random)
}
