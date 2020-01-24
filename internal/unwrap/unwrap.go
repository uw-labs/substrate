package unwrap

import "github.com/uw-labs/substrate"

// AnnotatedMessage is a convenience function that unwraps a chain of annotated messages
// and returns the original one.
func AnnotatedMessage(msg substrate.Message) substrate.Message {
	for {
		aMsg, ok := msg.(substrate.AnnotatedMessage)
		if !ok {
			return msg
		}
		msg = aMsg.OriginalMessage()
	}
}
