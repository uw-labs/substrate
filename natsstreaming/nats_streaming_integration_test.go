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

	"github.com/Shopify/toxiproxy"
	"github.com/google/uuid"
	stand "github.com/nats-io/nats-streaming-server/server"
	"github.com/nats-io/stan.go"
	"github.com/stretchr/testify/require"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/internal/testshared"
	"golang.org/x/sync/errgroup"
)

func TestAll(t *testing.T) {
	t.Skip("broken, nats will be removed in future version")

	k, err := runServer()
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = k.Kill()
	}()

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

		QueueGroup:             groupID,
		Subject:                topic,
		ConnectionNumPings:     3,
		ConnectionPingInterval: 1,
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
				_ = cmd.Process.Kill()
				return nil, fmt.Errorf("docker port returned something strange: %s", outS)
			}
			p, err := strconv.Atoi(strings.TrimSpace(ps[1]))
			if err != nil {
				_ = cmd.Process.Kill()
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
			// ks.Kill()
			// return nil, errors.New("what?")
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

func TestConsumerErrorOnBackendDisconnect(t *testing.T) {
	// seed nats with some test data
	stanServerOpts := stand.GetDefaultOptions()
	natsServerOpts := stand.DefaultNatsServerOptions
	natsServerOpts.Port = 10247 // sorry!
	natsServ, err := stand.RunServerWithOpts(stanServerOpts, &natsServerOpts)
	require.NoError(t, err)
	defer natsServ.Shutdown()
	conn, err := stan.Connect(stand.DefaultClusterID, "test-publish", stan.NatsURL(fmt.Sprintf("nats://localhost:%d", natsServerOpts.Port)))
	require.NoError(t, err)
	for i := 0; i < 1000; i++ {
		err := conn.Publish("test", []byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	// set up backend handler with a proxy in the connection
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	proxy := toxiproxy.NewProxy()
	proxy.Listen = "localhost:10248"
	proxy.Upstream = fmt.Sprintf("localhost:%d", natsServerOpts.Port)
	err = proxy.Start()
	require.NoError(t, err)
	asyncSource, err := NewAsyncMessageSource(AsyncMessageSourceConfig{
		AckWait:                time.Second * 30,
		ClientID:               "test",
		ClusterID:              stand.DefaultClusterID,
		MaxInFlight:            1,
		Offset:                 OffsetOldest,
		QueueGroup:             "test",
		Subject:                "test",
		URL:                    fmt.Sprintf("nats://%s", proxy.Listen),
		ConnectionNumPings:     3,
		ConnectionPingInterval: 1,
	})
	require.NoError(t, err)
	success := make(chan struct{})
	egrp, groupCtx := errgroup.WithContext(ctx)
	messageChan := make(chan substrate.Message)
	ackChan := make(chan substrate.Message)
	egrp.Go(func() error {
		for msg := range messageChan {
			val, _ := strconv.Atoi(string(msg.Data()))
			if val == 10 {
				t.Log("close proxy after 10 msgs")
				proxy.Stop() // close proxy after 10 msgs
			}
			ackChan <- msg
		}
		return nil
	})
	egrp.Go(func() error {
		err := asyncSource.ConsumeMessages(groupCtx, messageChan, ackChan)
		if err != nil && err != context.Canceled {
			t.Log(err)
			close(success) // our handler returned the error from the ping timeout
		}
		return err
	})
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-success:
	}
}

func TestProducerOnDisconnectedError(t *testing.T) {
	// seed nats with some test data
	stanServerOpts := stand.GetDefaultOptions()
	stanServerOpts.Debug = false
	stanServerOpts.EnableLogging = false
	natsServerOpts := stand.DefaultNatsServerOptions
	natsServerOpts.Debug = false
	natsServerOpts.NoLog = true
	natsServerOpts.Port = 10257 // sorry!
	natsServ, err := stand.RunServerWithOpts(stanServerOpts, &natsServerOpts)
	require.NoError(t, err)
	defer natsServ.Shutdown()

	// set up backend handler with a proxy in the connection
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	proxy := toxiproxy.NewProxy()
	proxy.Listen = "localhost:10258"
	proxy.Upstream = fmt.Sprintf("localhost:%d", natsServerOpts.Port)
	if err := proxy.Start(); err != nil {
		t.Fatal(err)
	}
	sink, err := NewAsyncMessageSink(AsyncMessageSinkConfig{
		URL:                    fmt.Sprintf("nats://%s", proxy.Listen),
		ClusterID:              stand.DefaultClusterID,
		ClientID:               "test-client",
		Subject:                "test-subject",
		ConnectionPingInterval: 1,
		ConnectionNumPings:     3,
	})
	// hnd, err := newNatsStreamingProduceHandler(
	// fmt.Sprintf("nats://%s", proxy.Listen), stand.DefaultClusterID, 1, 1, 3)
	require.NoError(t, err)
	success := make(chan struct{})
	egrp, groupCtx := errgroup.WithContext(ctx)
	acks := make(chan substrate.Message)
	messages := make(chan substrate.Message)
	egrp.Go(func() error {
		err := sink.PublishMessages(groupCtx, acks, messages)
		if err != nil && err != context.Canceled {
			close(success)
		}
		return err
	})
	time.AfterFunc(time.Second*1, func() {
		proxy.Stop()
	})
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-success:
	}
}
