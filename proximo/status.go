package proximo

import (
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/uw-labs/substrate"
)

func proximoStatus(conn *grpc.ClientConn) (*substrate.Status, error) {
	switch state := conn.GetState(); state {
	case connectivity.Idle, connectivity.Ready:
		return &substrate.Status{Working: true}, nil
	case connectivity.Connecting:
		return &substrate.Status{Working: true, Problems: []string{"connecting"}}, nil
	case connectivity.TransientFailure:
		return &substrate.Status{Working: true, Problems: []string{"transient failure"}}, nil
	case connectivity.Shutdown:
		return &substrate.Status{Working: false, Problems: []string{"connection shutdown"}}, nil
	default:
		return nil, errors.Errorf("unknown connection state: %s", state)
	}
}
