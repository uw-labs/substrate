package jetstream

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/suburl"
)

func init() {
	suburl.RegisterSink("jet-stream", newJetStreamSink)
	suburl.RegisterSource("jet-stream", newJetStreamSource)
}

func newJetStreamSink(u *url.URL) (substrate.AsyncMessageSink, error) {
	q := u.Query()

	natsURL := "http://" + u.Host

	topic := strings.Trim(u.Path, "/")
	if strings.Contains(topic, "/") {
		return nil, fmt.Errorf("error parsing topic from url (%s)", topic)
	}

	var maxPending int
	if v := q.Get("max-pending"); v != "" {
		m, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid max pending value: %w", err)
		}
		maxPending = m
	}

	return natsStreamingSinker(AsyncMessageSinkConfig{
		URL:        natsURL,
		Topic:      topic,
		MaxPending: maxPending,
	})
}

var natsStreamingSinker = NewAsyncMessageSink

func newJetStreamSource(u *url.URL) (substrate.AsyncMessageSource, error) {
	q := u.Query()

	natsURL := "http://" + u.Host

	topic := strings.Trim(u.Path, "/")
	if strings.Contains(topic, "/") {
		return nil, fmt.Errorf("error parsing topic from url (%s)", topic)
	}

	conf := AsyncMessageSourceConfig{
		URL:           natsURL,
		Topic:         topic,
		ConsumerGroup: q.Get("consumer-group"),
	}

	switch offset := q.Get("offset"); offset {
	case "newest":
		conf.Offset = OffsetNewest
	case "oldest":
		conf.Offset = OffsetOldest
	case "":
	default:
		return nil, fmt.Errorf("unknown offset value '%s'", offset)
	}

	ackWaitString := q.Get("ack-wait")
	if ackWaitString != "" {
		ackWait, err := time.ParseDuration(ackWaitString)
		if err != nil {
			return nil, err
		}
		conf.AckWait = ackWait
	}

	return natsStreamingSourcer(conf)
}

var natsStreamingSourcer = NewAsyncMessageSource
