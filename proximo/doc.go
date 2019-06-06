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
// The url structure is proximo://host:port/topic/
//
// Additionally, for sources, the following url parameters are available
//
//      offset             - The initial offset. Valid values are `newest` and `oldest`.
//      consumer-group     - The consumer group id
//      keep-alive-time    - The interval that a keep alive is performed at as a go duration
//      keep-alive-timeout - The go duration at which a keepalive will timeout [Default: 10s] (requires keep-alive-time to take effect, if keep-alive-time is not present this is ignored)
//
package proximo
