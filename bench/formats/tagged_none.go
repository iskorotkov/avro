//go:build !goexperiment.jsonv2

package formats

import "github.com/iskorotkov/avro/bench/format"

func tagged() []format.Format { return nil }
