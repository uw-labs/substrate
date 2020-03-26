// Package kafka provides kafka support for substrate
//
// Usage
//
// This package support two methods of use.  The first is to directly use this package. See the function documentation for more details.
//
// The second method is to use the suburl package. See https://godoc.org/github.com/uw-labs/substrate/suburl for more information.
//
// Using suburl
//
// The url structure is kafka://host:port/topic/
//
// The following url parameters are available:
//
//      broker - Specifies additional broker addresses in the form host%3Aport (where %3A is a url encoded ':')
//
// Additionally, for sources, the following url parameters are available
//
//      offset           - The initial offset. Valid values are `newest` and `oldest`.
//      consumer-group   - The consumer group id
//      metadata-refresh - How frequently to refresh the cluster metadata. E.g., '10s' '2m'
//
// Finally to debug behaviour of the underlying Sarama library you can set environment variable
//      SUBSTRATE_KAFKA_DEBUG_LOG=true
// to enable Sarama debug logging.

package kafka

import (
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

func init() {
	if strings.ToLower(os.Getenv("SUBSTRATE_KAFKA_DEBUG_LOG")) == "true" {
		sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	}
}
