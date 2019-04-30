// package natsstreaming implements the AsynchronousMessageSource and
// AsynchronousMessageSink interfaces from the substrate package.
//
// The url used to configure a nats streaming source or sink is using the
// "nats-streaming" scheme.
//
// The path component of the url specifies the subject to connect to.
//
// The following query string parameters are used for both source and sink
// configuration:
//
// "cluster-id": the nats streaming ClusterID
// "client-id": the nats streaming ClientID
//
// The following connection loss detection parameters are available on both source and sink:
// "ping-timeout": the number of seconds to wait for a ping
// "ping-num-tries": the number of times pings should time out before the
// connection is considered dead
//
// The following parameters specify the subscription configuration for a source:
// "queue-group": the subscriptions QueueGroup
// "max-in-flight": the maximum amount of messages nats streaming server sends
// to the client in a single batch
// "ack-wait": the time the natsstreaming server waits for messages in flight
// to be acked, before attempting redelivery

package natsstreaming
