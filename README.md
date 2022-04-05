Substrate
=========

[![go-doc](https://godoc.org/github.com/uw-labs/substrate?status.svg)](https://godoc.org/github.com/uw-labs/substrate)

Substrate is a simple thin abstraction for message publishing and consumption.  It presents a simple API set for durable, at-least-once message publishing and subscription, on a number of backend message broker types.

The API is not yet stable.

Current implementations and their status
----------------------------------------

| Implementation                           | Status        |
| ---------------------------------------- | ------------- |
| Apache Kafka                             | beta          |
| Proximo                                  | alpha         |
| Freezer                                  | alpha         |

Additional resources
----------------------------------------

* [substrate-tools](https://github.com/uw-labs/substrate-tools) - Provides wrappers and packages that are useful for various tasks, such as acknowledgement ordering and instrumentation.
