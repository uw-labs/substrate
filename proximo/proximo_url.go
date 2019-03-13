package proximo

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/suburl"
)

func init() {
	suburl.RegisterSink("proximo", newProximoSink)
	suburl.RegisterSource("proximo", newProximoSource)
}

func newProximoSink(u *url.URL) (substrate.AsyncMessageSink, error) {
	q := u.Query()

	topic := strings.Trim(u.Path, "/")

	if strings.Contains(topic, "/") {
		return nil, fmt.Errorf("error parsing topic from url (%s)", topic)
	}

	conf := AsyncMessageSinkConfig{
		Broker: u.Host,
		Topic:  topic,
	}

	switch q.Get("insecure") {
	case "true":
		conf.Insecure = true
	case "false":
		conf.Insecure = false
	default:
		conf.Insecure = false
	}

	return proximoSinker(conf)
}

var proximoSinker = NewAsyncMessageSink

func newProximoSource(u *url.URL) (substrate.AsyncMessageSource, error) {
	q := u.Query()

	topic := strings.Trim(u.Path, "/")

	if strings.Contains(topic, "/") {
		return nil, fmt.Errorf("error parsing topic from url (%s)", topic)
	}

	conf := AsyncMessageSourceConfig{
		Broker:        u.Host,
		ConsumerGroup: q.Get("consumer-group"),
		Topic:         topic,
	}

	switch q.Get("insecure") {
	case "true":
		conf.Insecure = true
	case "false":
		conf.Insecure = false
	default:
		conf.Insecure = false
	}

	switch q.Get("offset") {
	case "newest":
		conf.Offset = OffsetNewest
	case "oldest":
		conf.Offset = OffsetOldest
	case "":
	default:
		return nil, fmt.Errorf("unknown offset value '%s'", q.Get("offset"))
	}

	return proximoSourcer(conf)
}

var proximoSourcer = NewAsyncMessageSource
