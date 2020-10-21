// Package proximo provides proximo support for substrate
//
// Usage
//
// This package support two methods of use.  The first is to directly use this package. See the function documentation for more details.
//
// The second method is to use the suburl package. See https://godoc.org/github.com/uw-labs/substrate/suburl for more information.
//
// Using suburl
//
// The url structure is proximo://clientID:secret@host:port/topic/
//
// The clientID and secret should be specified when proximo is setup with ACL.
//
// Additionally, for sources, the following url parameters are available
//
//      offset             - The initial offset. Valid values are `newest` and `oldest`.
//      consumer-group     - The consumer group id
//      keep-alive-time    - The interval that a keep alive is performed at as a go duration
//      keep-alive-timeout - The go duration at which a keepalive will timeout [Default: 10s] (requires keep-alive-time to take effect, if keep-alive-time is not present this is ignored)
//      insecure=true      - The connection to the proximo grpc endpoint will not be using TLS
//      max-recv-msg-size  - The gRPC max receive message size in bytes (source only) [Default: 67,108,864 (64MiB)]
//
package proximo
