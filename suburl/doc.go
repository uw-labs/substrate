// Package suburl provides a generic URL based interface for obtaining substrate
// source and sink objects.
//
// Usage
//
// Here is an example of obtaining a kafka sink.
//
//      sink, err := suburl.NewSink("kafka://localhost:9092/my-topic/?broker=localhost%3A9092&broker=localhost%3A9092")
//      if err != nil {
//              // error handling
//      }
//      defer sink.Close()
//
//      // use sink here
//
// To use a particular url scheme, the driver has to be registered, for example:
//
//      import _ "github.com/uw-labs/substrate/kafka"
//
// More complete documentation of options
//
// For a full description of the url options available for each scheme, see the documentation for the particular driver,
// for example https://godoc.org/github.com/uw-labs/substrate/kafka
package suburl
