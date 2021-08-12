package jetstream

import (
	"fmt"

	"github.com/nats-io/nats.go"

	"github.com/uw-labs/substrate"
)

func natsStatus(nc *nats.Conn) (*substrate.Status, error) {
	if nc == nil {
		return &substrate.Status{
			Problems: []string{"no nats connection"},
			Working:  false,
		}, nil
	}

	if nc.IsConnected() {
		return &substrate.Status{
			Working: true,
		}, nil
	}

	return &substrate.Status{
		Problems: []string{fmt.Sprintf("nats not connected - last error: %v", nc.LastError())},
		Working:  false,
	}, nil
}
