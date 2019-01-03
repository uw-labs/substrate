package natsstreaming

import (
	"fmt"

	nats "github.com/nats-io/go-nats"
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
		Problems: []string{fmt.Sprintf("nats not connected - last error: %v", lastErr)},
		Working:  false,
	}, nil
}
