package substrate

import "context"

// Option aggregates the client options
type Option interface {
	apply(*funcOption)
	WithRehydration() (bool, context.Context, chan<- struct{})
}

type funcOption struct {
	f                  func(*funcOption)
	rehydration        bool
	rehydrationContext context.Context
	rehydrationChannel chan<- struct{}
}

func (fdo *funcOption) WithRehydration() (bool, context.Context, chan<- struct{}) {
	return fdo.rehydration, fdo.rehydrationContext, fdo.rehydrationChannel
}

func (fdo *funcOption) apply(do *funcOption) {
	fdo.f(do)
}

func newFuncOption(f func(*funcOption)) *funcOption {
	return &funcOption{
		f: f,
	}
}

// ParseOptions parse the given options and mutate the options structure
func ParseOptions(opts ...Option) Option {
	o := new(funcOption)
	for _, opt := range opts {
		opt.apply(o)
	}
	return o
}

// WithRehydration is used while rehydrating an aggregate based on a stream of events.
// Once the rehydration is done (the latest sequence is reached), a notification is sent to the provided channel.
func WithRehydration(ctx context.Context, rehydrated chan<- struct{}) Option {
	return newFuncOption(func(options *funcOption) {
		options.rehydration = true
		options.rehydrationContext = ctx
		options.rehydrationChannel = rehydrated
	})
}
