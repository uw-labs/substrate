package natsstreaming

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
	suburl.RegisterSink("nats-streaming", newNatsStreamingSink)
	suburl.RegisterSource("nats-streaming", newNatsStreamingSource)
}

func newNatsStreamingSink(u *url.URL) (substrate.AsyncMessageSink, error) {
	q := u.Query()

	natsURL := "nats://" + u.Host

	subject := strings.Trim(u.Path, "/")
	if strings.Contains(subject, "/") {
		return nil, fmt.Errorf("error parsing subject from url (%s)", subject)
	}

	to := getTimeoutConfig(q)

	return natsStreamingSinker(AsyncMessageSinkConfig{
		URL:                    natsURL,
		ClusterID:              q.Get("cluster-id"),
		ClientID:               q.Get("client-id"),
		Subject:                subject,
		ConnectionPingInterval: to.seconds,
		ConnectionNumPings:     to.tries,
	})
}

var natsStreamingSinker = NewAsyncMessageSink

func newNatsStreamingSource(u *url.URL) (substrate.AsyncMessageSource, error) {
	q := u.Query()

	natsURL := "nats://" + u.Host

	subject := strings.Trim(u.Path, "/")
	if strings.Contains(subject, "/") {
		return nil, fmt.Errorf("error parsing topic from url (%s)", subject)
	}

	toConf := getTimeoutConfig(q)

	conf := AsyncMessageSourceConfig{
		URL:                    natsURL,
		ClusterID:              q.Get("cluster-id"),
		QueueGroup:             q.Get("queue-group"),
		ClientID:               q.Get("client-id"),
		Subject:                subject,
		ConnectionPingInterval: toConf.seconds,
		ConnectionNumPings:     toConf.tries,
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

	maxInflightString := q.Get("max-in-flight")
	if maxInflightString != "" {
		maxInflight, err := strconv.Atoi(maxInflightString)
		if err != nil {
			return nil, err
		}
		conf.MaxInFlight = maxInflight
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

func getTimeoutConfig(q url.Values) connectionTimeOutConfig {
	seconds, err := strconv.Atoi(q.Get("ping-timeout"))
	if err != nil {
		seconds = 1
	}
	tries, err := strconv.Atoi(q.Get("ping-num-tries"))
	if err != nil {
		tries = 3
	}
	return connectionTimeOutConfig{
		seconds: seconds,
		tries:   tries,
	}
}

var natsStreamingSourcer = NewAsyncMessageSource
