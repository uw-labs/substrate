package kafka

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/suburl"
)

func init() {
	suburl.RegisterSink("kafka", newKafkaSink)
	suburl.RegisterSource("kafka", newKafkaSource)
}

func newKafkaSink(u *url.URL) (substrate.AsyncMessageSink, error) {
	conf, err := NewAsyncMessageSinkConfig(u)
	if err != nil {
		return nil, fmt.Errorf("failed to create new message sink config %s", err.Error())
	}

	return kafkaSinker(conf)
}

//NewAsyncMessageSinkConfig allows building config and further modifying if required with funcKey, etc.
func NewAsyncMessageSinkConfig(u *url.URL) (AsyncMessageSinkConfig, error) {
	q := u.Query()
	topic := strings.Trim(u.Path, "/")
	if strings.Contains(topic, "/") {
		return AsyncMessageSinkConfig{}, fmt.Errorf("error parsing topic from url (%s)", topic)
	}
	conf := AsyncMessageSinkConfig{
		Brokers: []string{u.Host},
		Topic:   topic,
	}
	conf.Brokers = append(conf.Brokers, q["broker"]...)

	return conf, nil
}

var kafkaSinker = NewAsyncMessageSink

func newKafkaSource(u *url.URL) (substrate.AsyncMessageSource, error) {
	q := u.Query()

	topic := strings.Trim(u.Path, "/")

	if strings.Contains(topic, "/") {
		return nil, fmt.Errorf("error parsing topic from url (%s)", topic)
	}

	conf := AsyncMessageSourceConfig{
		Brokers:       []string{u.Host},
		ConsumerGroup: q.Get("consumer-group"),
		Topic:         topic,
	}

	conf.Brokers = append(conf.Brokers, q["broker"]...)

	switch q.Get("offset") {
	case "newest":
		conf.Offset = OffsetNewest
	case "oldest":
		conf.Offset = OffsetOldest
	case "":
	default:
		return nil, fmt.Errorf("ignoring unknown offset value '%s'", q.Get("offset"))
	}

	dur := q.Get("metadata-refresh")
	if dur != "" {
		d, err := time.ParseDuration(dur)
		if err != nil {
			return nil, fmt.Errorf("failed to parse refresh duration : %v", err)
		}
		conf.MetadataRefreshFrequency = d
	}

	return kafkaSourcer(conf)
}

var kafkaSourcer = NewAsyncMessageSource
