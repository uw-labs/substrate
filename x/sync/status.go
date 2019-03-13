package sync

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/uw-labs/substrate"
)

var (
	// ErrSinkAlreadyClosed is an error returned when user tries to publish a message or
	// close a sink after it was already closed.
	ErrSinkAlreadyClosed = errors.New("sink was closed already")
	// ErrSinkClosedOrFailedDuringSend is an error returned when a sink is closed or fails while sending a message.
	ErrSinkClosedOrFailedDuringSend = errors.New("sink was closed or failed while sending the message")

	// ErrDisconnected is an error signifying that the backend of a synchronous adapter was disconnected
	ErrDisconnected = errors.New("async backend was disconnected")
	// ErrNotConnected is an error signifying that synchronous adapter is not connected to any backend
	ErrNotConnected = errors.New("not connected to any async backend")
	// ErrAlreadyStarted is an error signifying that synchronous adapter has already started
	ErrAlreadyStarted = errors.New("sync adapter is already running")
)

// checkBackend periodically checks status of the provided statuser
func checkBackend(ctx context.Context, statuser substrate.Statuser) error {
	ticker := time.NewTicker(time.Second * 20) // TODO: maybe make this configurable
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s, err := statuser.Status()
			if err != nil {
				return err
			}
			if !s.Working {
				return ErrDisconnected
			}
		}
	}
}
