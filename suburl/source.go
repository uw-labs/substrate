package suburl

import (
	"fmt"
	"github.com/uw-labs/substrate"
	"net/url"
	"sync"
)

var (
	sourceTypesMu sync.RWMutex
	sourceTypes   = make(map[string]func(url *url.URL) (substrate.AsyncMessageSource, error))
)

func RegisterSource(scheme string, sinkFunc func(url *url.URL) (substrate.AsyncMessageSource, error)) {
	sourceTypesMu.Lock()
	defer sourceTypesMu.Unlock()
	if sinkFunc == nil {
		panic("pubsub: sink function is nil")
	}
	if _, dup := sourceTypes[scheme]; dup {
		panic("pubsub: RegisterSink called more than once for sink " + scheme)
	}
	sourceTypes[scheme] = sinkFunc
}

// NewSource returns a message source based on the supplied URL.
func NewSource(u string) (substrate.AsyncMessageSource, error) {
	parsed, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	sourceTypesMu.RLock()
	defer sourceTypesMu.RUnlock()

	f := sourceTypes[parsed.Scheme]
	if f == nil {
		return nil, fmt.Errorf("unknown scheme : %s", parsed.Scheme)
	}

	return f(parsed)
}
