package freezer

import (
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
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
	case "zstd":
		ct = freezer.CompressionTypeZstd
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

	var (
		store straw.StreamStore
		err   error
	)
	switch u.Scheme {
	case "freezer+dir":
		store, err = strawOpen("file:///")
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
		store, err = strawOpen(u1.String())

	default:
		return nil, fmt.Errorf("unsupported scheme : %s", u.Scheme)
	}
	if err != nil {
		return nil, err
	}
	conf := AsyncMessageSinkConfig{
		StreamStore:          store,
		MaxUnflushedMessages: maxUnflushed,
		FreezerConfig: freezer.MessageSinkConfig{
			Path:            u.Path,
			CompressionType: ct,
		},
	}
	sink, err := sinker(conf)
	if err != nil {
		return nil, err
	}
	return closeStoreSink{
		AsyncMessageSink: sink,
		store:            store,
	}, nil
}

var sinker = NewAsyncMessageSink

type closeStoreSink struct {
	substrate.AsyncMessageSink
	store straw.StreamStore
}

func (sink closeStoreSink) Close() (err error) {
	return closeAll([]io.Closer{sink.AsyncMessageSink, sink.store})
}

func newFreezerSource(u *url.URL) (substrate.AsyncMessageSource, error) {
	q := u.Query()

	cts := q.Get("compression")
	ct := freezer.CompressionTypeNone
	switch cts {
	case "snappy":
		ct = freezer.CompressionTypeSnappy
	case "zstd":
		ct = freezer.CompressionTypeZstd
	case "none", "":
	default:
		return nil, fmt.Errorf("unknown compression type : %s", cts)
	}

	var (
		store straw.StreamStore
		err   error
	)

	switch u.Scheme {
	case "freezer+dir":
		store, err = strawOpen("file:///")
		if err != nil {
			return nil, err
		}
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
		store, err = strawOpen(u1.String())

	default:
		return nil, fmt.Errorf("unsupported scheme : %s", u.Scheme)
	}
	if err != nil {
		return nil, err
	}

	conf := AsyncMessageSourceConfig{
		StreamStore: store,
		FreezerConfig: freezer.MessageSourceConfig{
			Path:            u.Path,
			PollPeriod:      10 * time.Second,
			CompressionType: ct,
		},
	}
	source, err := sourcer(conf)
	if err != nil {
		return nil, err
	}
	return &closeStoreSource{
		AsyncMessageSource: source,
		store:              store,
	}, nil
}

var sourcer = NewAsyncMessageSource

type closeStoreSource struct {
	substrate.AsyncMessageSource
	store straw.StreamStore
}

func (sink closeStoreSource) Close() (err error) {
	return closeAll([]io.Closer{sink.AsyncMessageSource, sink.store})
}

var strawOpen = straw.Open

func closeAll(closers []io.Closer) (outErr error) {
	for _, c := range closers {
		if err := c.Close(); err != nil {
			outErr = multierror.Append(outErr, err)
		}
	}
	return outErr
}
