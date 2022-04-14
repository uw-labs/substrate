package debug

import (
	"encoding/base64"
	"hash/fnv"
	"log"
	"os"
	"strconv"

	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

var verbose bool

func init() {
	verbose, _ = strconv.ParseBool(os.Getenv("VERBOSE"))
}

func messageHash(data []byte) string {
	f := fnv.New32()
	_, _ = f.Write(data)
	sum := f.Sum(nil)
	return base64.StdEncoding.EncodeToString(sum)
}

type Debugger struct {
	Enabled bool
}

func (d *Debugger) Logf(s string, args ...interface{}) {
	if !verbose {
		return
	}
	d.doLogf(s, args)
}

func (d *Debugger) doLogf(s string, args ...interface{}) {
	args1 := make([]interface{}, len(args))
	for i, arg := range args {
		switch a := arg.(type) {
		case substrate.Message:
			args1[i] = messageHash(a.Data())
		case *proto.Message:
			args1[i] = messageHash(a.Data)
		case []byte:
			args1[i] = messageHash(a)
		default:
			args1[i] = arg
		}
	}
	log.Printf(s, args1...)
}
