package proximo

import (
	"fmt"
	"net/url"
	"strings"
	"time"

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

	ka, err := keepAliveFromURLValues(q)
	if err != nil {
		return nil, fmt.Errorf("unable to generate keep alive config: %s", err.Error())
	}
	conf.KeepAlive = ka

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

	ka, err := keepAliveFromURLValues(q)
	if err != nil {
		return nil, fmt.Errorf("unable to generate keep alive config: %s", err.Error())
	}
	conf.KeepAlive = ka

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

func keepAliveFromURLValues(q url.Values) (*KeepAlive, error) {

	if q.Get("keep-alive-time") != "" {

		t, err := time.ParseDuration(q.Get("keep-alive-time"))
		if err != nil {
			return nil, fmt.Errorf("unable to parse keep-alive-time: %s", q.Get("keep-alive-time"))
		}

		ka := &KeepAlive{
			Time:    t,
			Timeout: time.Second * 10,
		}

		if q.Get("keep-alive-timeout") != "" {
			to, err := time.ParseDuration(q.Get("keep-alive-timeout"))
			if err != nil {
				return nil, fmt.Errorf("unable to parse keep-alive-timeout: %s", q.Get("keep-alive-timeout"))
			}
			ka.Timeout = to
		}

		return ka, nil
	}

	return nil, nil
}

var proximoSourcer = NewAsyncMessageSource
