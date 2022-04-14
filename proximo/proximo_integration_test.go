package proximo

import (
	"context"
	"fmt"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/internal/testshared"
)

func TestAll(t *testing.T) {
	k, err := runServer(t)
	if err != nil {
		t.Fatal(err)
	}

	testshared.TestAll(t, k)
}

type testServer struct {
	address string
	port    int
}

func (ts *testServer) NewConsumer(topic string, groupID string) substrate.AsyncMessageSource {
	s, err := NewAsyncMessageSource(AsyncMessageSourceConfig{
		Broker:        fmt.Sprintf("%s:%d", ts.address, ts.port),
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
		Broker:   fmt.Sprintf("%s:%d", ts.address, ts.port),
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

func runServer(t *testing.T) (*testServer, error) {
	t.Helper()

	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "quay.io/utilitywarehouse/uw-proximo:latest",

			ExposedPorts: []string{
				"6868/tcp",
				"8080/tcp",
			},
			Cmd: []string{
				"mem",
			},
			WaitingFor: wait.
				NewHTTPStrategy("/__/health").
				WithPort(nat.Port(fmt.Sprintf("%d", 8080))),
		},
		ProviderType: testcontainers.ProviderDocker,
		Started:      true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Fatal(err)
		}
	})

	ip, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}

	mappedPort, err := container.MappedPort(ctx, "6868")
	if err != nil {
		t.Fatal(err)
	}

	return &testServer{
		port:    mappedPort.Int(),
		address: ip,
	}, nil
}
