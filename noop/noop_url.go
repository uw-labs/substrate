package noop

import (
	"net/url"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/suburl"
)

func init() {
	suburl.RegisterSink("noop", newNoopSink)
	suburl.RegisterSource("noop", newNoopSource)
}

func newNoopSink(u *url.URL) (substrate.AsyncMessageSink, error) {
	return NewAsyncMessageSink(), nil
}

func newNoopSource(u *url.URL) (substrate.AsyncMessageSource, error) {
	return NewAsyncMessageSource(), nil
}
