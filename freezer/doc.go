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
//
// The following url parameters are available:
//
//      compression - Specifies the type of compression to use.  Valid values are `none` and `snappy`. The default is `none`.
//
package freezer
