package freezer

import (
	"fmt"
	"net/url"
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

	var streamstore straw.StreamStore
	switch u.Scheme {
	case "freezer+dir":
		streamstore = &straw.OsStreamStore{}
	case "freezer+s3":
		var enc straw.S3Option
		sse := q.Get("sse")
		if sse == "aes256" {
			enc = straw.S3ServerSideEncoding(straw.ServerSideEncryptionTypeAES256)
		}
		if sse != "" && sse != "aes256" {
			return nil, fmt.Error("unsupported value: %s passed for sse parameter", sse)
		}
		var err error
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
		streamstore: streamstore,
		fconfig: freezer.MessageSinkConfig{
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
		conf := AsyncMessageSourceConfig{
			streamstore: &straw.OsStreamStore{},
			fconfig: freezer.MessageSourceConfig{
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
			streamstore: ss,
			fconfig: freezer.MessageSourceConfig{
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
