// Package freezer provides freezer support for substrate
//
// Usage
//
// This package support two methods of use.  The first is to directly use this package. See the function documentation for more details.
//
// The second method is to use the suburl package. See https://godoc.org/github.com/uw-labs/substrate/suburl for more information.
//
// Using suburl
//
// The url structure is freezer+s3://bucket/
//                   or freezer+dir://full/path/
//
// On the sink there's an additional url parameter that allows one to specify the server side encryption flag for s3.
// It currently only supports aes256.
// It then looks like this: freezer+s3://bucket/path?sse=aes256
//
// The following url parameters are available:
//
//      compression - Specifies the type of compression to use.  Valid values are `none` and `snappy`. The default is `none`.
//      max_unflushed - Specifies the maximum number of unflushed messages.  Default is 1024
//
package freezer
