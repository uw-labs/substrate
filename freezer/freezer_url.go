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
	suburl.RegisterSink("freezer+dir", newKafkaSink)
	suburl.RegisterSource("freezer+dir", newKafkaSource)
	suburl.RegisterSource("freezer+s3", newKafkaSource)
}

func newKafkaSink(u *url.URL) (substrate.AsyncMessageSink, error) {

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
		conf := AsyncMessageSinkConfig{
			streamstore: &straw.OsStreamStore{},
			fconfig: freezer.MessageSinkConfig{
				Path:            u.Path,
				CompressionType: ct,
			},
		}
		return sinker(conf)
	default:
		return nil, fmt.Errorf("unsupported scheme : %s", u.Scheme)
	}
}

var sinker = NewAsyncMessageSink

func newKafkaSource(u *url.URL) (substrate.AsyncMessageSource, error) {
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
