package proximo

import (
	"crypto/tls"
	"time"

	"google.golang.org/grpc/keepalive"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"

	"github.com/uw-labs/substrate"
)

type KeepAlive struct {
	Time    time.Duration
	Timeout time.Duration
}

func dialProximo(broker string, insecure bool, ka *KeepAlive) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption

	if ka != nil {
		opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    ka.Time,
			Timeout: ka.Timeout,
		}))
	}

	if insecure {
		opts = append(opts, grpc.WithInsecure())
	} else {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(new(tls.Config))))
	}
	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*64)))

	conn, err := grpc.Dial(broker, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial %s", broker)
	}

	return conn, nil
}

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
