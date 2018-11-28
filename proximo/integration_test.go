package proximo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os/exec"
	"testing"

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

func (ks *testServer) NewConsumer(topic string, groupID string) substrate.AsyncMessageSource {
	s, err := NewAsyncMessageSource(AsyncMessageSourceConfig{
		Broker:        fmt.Sprintf("localhost:%d", ks.port),
		ConsumerGroup: groupID,
		Topic:         topic,
		//	Offset:        OffsetOldest,
		Insecure: true,
	})

	if err != nil {
		panic(err)
	}
	return s
}

func (ks *testServer) NewProducer(topic string) substrate.AsyncMessageSink {
	s, err := NewAsyncMessageSink(AsyncMessageSinkConfig{
		Broker: fmt.Sprintf("localhost:%d", ks.port),
		Topic:  topic,
	})
	if err != nil {
		panic(err)
	}
	return s
}

func (ks *testServer) TestEnd() {}

func (ks *testServer) Kill() error {
	return ks.cmd.Process.Kill()
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

	ks := &testServer{cmd, 6868}

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
