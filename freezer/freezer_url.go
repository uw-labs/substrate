package freezer

import (
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/pkg/errors"
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

func newFreezerSink(u *url.URL) (sink substrate.AsyncMessageSink, err error) {

	q := u.Query()

	var (
		maxUnflushedCount  = defaultMaxUnflushedCount
		maxUnflushedPeriod = defaultMaxUnflushedPeriod
	)
	if count := q.Get("max-unflushed-count"); count != "" {
		maxUnflushedCount, err = strconv.Atoi(count)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse max-unflushed-count")
		}
	}
	if period := q.Get("max-unflushed-period"); period != "" {
		maxUnflushedPeriod, err = time.ParseDuration(period)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse max-unflushed-period")
		}
	}

	cts := q.Get("compression")
	ct := freezer.CompressionTypeNone
	switch cts {
	case "snappy":
		ct = freezer.CompressionTypeSnappy
	case "none", "":
	default:
		return nil, fmt.Errorf("unknown compression type : %s", cts)
	}

	var streamstore straw.StreamStore
	switch u.Scheme {
	case "freezer+dir":
		streamstore = &straw.OsStreamStore{}
	case "freezer+s3":
		var (
			enc straw.S3Option
			err error
		)

		sse := q.Get("sse")
		switch sse {
		case "":
		case "aes256":
			enc = straw.S3ServerSideEncoding(straw.ServerSideEncryptionTypeAES256)
		default:
			return nil, fmt.Errorf("unsupported value: %s passed for sse parameter", sse)
		}

		if enc != nil {
			streamstore, err = straw.NewS3StreamStore(u.Hostname(), enc)
		} else {
			streamstore, err = straw.NewS3StreamStore(u.Hostname())
		}
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported scheme : %s", u.Scheme)
	}
	conf := AsyncMessageSinkConfig{
		StreamStore: streamstore,
		FreezerConfig: freezer.MessageSinkConfig{
			Path:            u.Path,
			CompressionType: ct,
		},
		MaxUnflushedCount:  maxUnflushedCount,
		MaxUnflushedPeriod: maxUnflushedPeriod,
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
		conf := AsyncMessageSourceConfig{
			StreamStore: &straw.OsStreamStore{},
			FreezerConfig: freezer.MessageSourceConfig{
				Path:            u.Path,
				PollPeriod:      10 * time.Second,
				CompressionType: ct,
			},
		}
		return sourcer(conf)
	case "freezer+s3":
		ss, err := straw.NewS3StreamStore(u.Hostname())
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
