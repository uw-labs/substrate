// Package natsstreaming provides kafka support for substrate
//
// Usage
//
// This package support two methods of use.  The first is to directly use this package. See the function documentation for more details.
//
// The second method is to use the suburl package. See https://godoc.org/github.com/uw-labs/substrate/suburl for more information.
//
// Using suburl
//
// The url structure is nats-streaming://host:port/subject/
//
// The following url parameters are available:
//
//      cluster-id  - The nats streaming cluster id
//      client-id   - The nats streaming client id
//
// Additionally, for sources, the following url parameters are available
//
//      queue-group      - The nats streaming queue group
//      max-in-flight    - The nats streaming MaxInFlight value
//      ack-wait         - The nats streaming AckWait duration, e.g., '30s', '2m'
//
package natsstreaming
