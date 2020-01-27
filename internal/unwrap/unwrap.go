package unwrap

import "github.com/uw-labs/substrate"

// AnnotatedMessage is an interface implemented by messages used by sink wrappers,
// in particular the synchronous sink adapter. This allows the backends to retrieve
// the original message the user sent when invoking user provided callbacks for example
// for getting partition keys.
type AnnotatedMessage interface {
	substrate.Message
	Original() substrate.Message
}

// Unwrap is a convenience function that unwraps a chain of annotated messages
// and returns the original one.
func Unwrap(msg substrate.Message) substrate.Message {
	for {
		aMsg, ok := msg.(AnnotatedMessage)
		if !ok {
			return msg
		}
		msg = aMsg.Original()
	}
}
