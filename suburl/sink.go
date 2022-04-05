package suburl

import (
	"fmt"
	"net/url"
	"sync"

	"github.com/uw-labs/substrate"
)

var (
	sinkTypesMu sync.RWMutex
	sinkTypes   = make(map[string]func(url *url.URL) (substrate.AsyncMessageSink, error))
)

func RegisterSink(scheme string, sinkFunc func(url *url.URL) (substrate.AsyncMessageSink, error)) {
	sinkTypesMu.Lock()
	defer sinkTypesMu.Unlock()
	if sinkFunc == nil {
		panic("pubsub: sink function is nil")
	}
	if _, dup := sinkTypes[scheme]; dup {
		panic("pubsub: RegisterSink called more than once for sink " + scheme)
	}
	sinkTypes[scheme] = sinkFunc
}

// NewSink will return a message sink based on the supplied URL.
// Examples:
//  kafka://localhost:123/my-topic/?metadata-refresh=2s&broker=localhost:234&broker=localhost:345
func NewSink(u string) (substrate.AsyncMessageSink, error) {
	parsed, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	sinkTypesMu.RLock()
	defer sinkTypesMu.RUnlock()

	f := sinkTypes[parsed.Scheme]
	if f == nil {
		return nil, fmt.Errorf("unknown scheme : %s", parsed.Scheme)
	}
	return f(parsed)
}
