Substrate
=========

[![go-doc](https://godoc.org/github.com/uw-labs/substrate?status.svg)](https://godoc.org/github.com/uw-labs/substrate)

Substrate is a simple thin abstraction for message publishing and consumption.  It presents a simple API set for durable, at-least-once message publishing and subscription, on a number of backend message broker types.

The API is not yet stable.

Current implementations and their status
----------------------------------------

| Implementation                           | Status        |
| ---------------------------------------- | ------------- |
| Amazon SQS                               | beta          |
| Apache Kafka                             | beta          |
| Nats streaming                           | beta          |
| Proximo                                  | alpha         |
| Freezer                                  | alpha         |
