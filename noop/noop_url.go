package noop

import (
	"net/url"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/suburl"
)

func init() {
	suburl.RegisterSink("noop", newNoopSink)
}

func newNoopSink(u *url.URL) (substrate.AsyncMessageSink, error) {
	return NewAsyncMessageSink()
}
