//go:build goexperiment.jsonv2

package formats

import (
	"github.com/iskorotkov/avro/bench/format"
	"github.com/iskorotkov/avro/bench/formats/stdjsonv2"
)

func tagged() []format.Format {
	return []format.Format{stdjsonv2.Format{}}
}
