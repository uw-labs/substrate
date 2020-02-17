package freezer

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/uw-labs/freezer"
	"github.com/uw-labs/straw"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/suburl"
)

func init() {
	suburl.RegisterSink("freezer+dir", newFreezerSink)
	suburl.RegisterSource("freezer+dir", newFreezerSource)
	suburl.RegisterSink("freezer+s3", newFreezerSink)
	suburl.RegisterSource("freezer+s3", newFreezerSource)
}

func newFreezerSink(u *url.URL) (substrate.AsyncMessageSink, error) {

	q := u.Query()

	cts := q.Get("compression")
	ct := freezer.CompressionTypeNone
	switch cts {
	case "snappy":
		ct = freezer.CompressionTypeSnappy
	case "none", "":
	default:
		return nil, fmt.Errorf("unknown compression type : %s", cts)
	}

	maxUnflushedStr := q.Get("max_unflushed")
	var maxUnflushed int
	if maxUnflushedStr != "" {
		var err error
		maxUnflushed, err = strconv.Atoi(maxUnflushedStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse max_unflushed value '%s'", maxUnflushedStr)
		}
	}

	var streamstore straw.StreamStore
	var err error
	switch u.Scheme {
	case "freezer+dir":
		streamstore, err = strawOpen("file:///")
		if err != nil {
			return nil, err
		}
	case "freezer+s3":
		u1 := url.URL{
			Scheme: "s3",
			Host:   u.Host,
		}

		// carry through a whitelist of query params for the straw URL
		newVals := url.Values(make(map[string][]string))
		for k, vals := range u.Query() {
			for _, val := range vals {
				switch k {
				case "sse":
					// substrate has always allowed & expected lower case values such as
					// `aes256` but straw expects upper case.  Convert here so that either
					// works with substrate now.
					newVals.Add(k, strings.ToUpper(val))
				}
			}
		}

		u1.RawQuery = newVals.Encode()

		streamstore, err = strawOpen(u1.String())
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported scheme : %s", u.Scheme)
	}
	conf := AsyncMessageSinkConfig{
		StreamStore:          streamstore,
		MaxUnflushedMessages: maxUnflushed,
		FreezerConfig: freezer.MessageSinkConfig{
			Path:            u.Path,
			CompressionType: ct,
		},
	}
	return sinker(conf)
}

var sinker = NewAsyncMessageSink

func newFreezerSource(u *url.URL) (substrate.AsyncMessageSource, error) {
	q := u.Query()

	cts := q.Get("compression")
	ct := freezer.CompressionTypeNone
	switch cts {
	case "snappy":
		ct = freezer.CompressionTypeSnappy
	case "none", "":
	default:
		return nil, fmt.Errorf("unknown compression type : %s", cts)
	}

	switch u.Scheme {
	case "freezer+dir":
		ss, err := strawOpen("file:///")
		if err != nil {
			return nil, err
		}
		conf := AsyncMessageSourceConfig{
			StreamStore: ss,
			FreezerConfig: freezer.MessageSourceConfig{
				Path:            u.Path,
				PollPeriod:      10 * time.Second,
				CompressionType: ct,
			},
		}
		return sourcer(conf)
	case "freezer+s3":
		u1 := url.URL{Scheme: "s3", Host: u.Hostname()}

		// carry through a whitelist of query params for the straw URL
		newVals := url.Values(make(map[string][]string))
		for k, vals := range u.Query() {
			for _, val := range vals {
				switch k {
				case "sse":
					// substrate has always allowed & expected lower case values such as
					// `aes256` but straw expects upper case.  Convert here so that either
					// works with substrate now.
					newVals.Add(k, strings.ToUpper(val))
				}
			}
		}
		u1.RawQuery = newVals.Encode()

		ss, err := strawOpen(u1.String())
		if err != nil {
			return nil, err
		}
		conf := AsyncMessageSourceConfig{
			StreamStore: ss,
			FreezerConfig: freezer.MessageSourceConfig{
				Path:            u.Path,
				PollPeriod:      10 * time.Second,
				CompressionType: ct,
			},
		}
		return sourcer(conf)
	default:
		return nil, fmt.Errorf("unsupported scheme : %s", u.Scheme)
	}

}

var sourcer = NewAsyncMessageSource

var strawOpen = straw.Open
