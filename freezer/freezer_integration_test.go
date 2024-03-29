package freezer

import (
	"os"
	"testing"

	"github.com/uw-labs/freezer"
	"github.com/uw-labs/straw"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/internal/testshared"
)

func TestAll(t *testing.T) {
	t.Skip("broken")

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
	ss  straw.StreamStore
	dir string
}

func runServer() (*testServer, error) {
	dir, err := os.MkdirTemp("/tmp/", "substrate_freezer_test")
	if err != nil {
		panic(err)
	}

	ss, _ := straw.Open("file:///")

	return &testServer{
		ss,
		dir,
	}, nil
}

func (ks *testServer) NewConsumer(topic string, groupID string) substrate.AsyncMessageSource {
	s, err := NewAsyncMessageSource(AsyncMessageSourceConfig{
		StreamStore: ks.ss,
		FreezerConfig: freezer.MessageSourceConfig{
			Path: ks.dir,
		},
	})
	if err != nil {
		panic(err)
	}
	return s
}

func (ks *testServer) NewProducer(topic string) substrate.AsyncMessageSink {
	s, err := NewAsyncMessageSink(AsyncMessageSinkConfig{
		StreamStore: ks.ss,
		FreezerConfig: freezer.MessageSinkConfig{
			Path: ks.dir,
		},
	})
	if err != nil {
		panic(err)
	}
	return s
}

func (ks *testServer) TestEnd() {
	_ = os.RemoveAll(ks.dir)
	_ = os.MkdirAll(ks.dir, 0755)
}

func (ks *testServer) Kill() error {
	// log.Printf("TODO: remove %s\n", ks.dir)
	os.RemoveAll(ks.dir)
	return nil
}
