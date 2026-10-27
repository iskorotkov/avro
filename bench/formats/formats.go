// Package formats lists every compared serialisation format.
package formats

import (
	"github.com/iskorotkov/avro/bench/format"
	"github.com/iskorotkov/avro/bench/formats/avro"
	"github.com/iskorotkov/avro/bench/formats/hamba"
	"github.com/iskorotkov/avro/bench/formats/protobuf"
	"github.com/iskorotkov/avro/bench/formats/stdjson"
)

// All returns every format, the baseline first.
func All() []format.Format {
	return []format.Format{
		avro.Format{},
		hamba.Format{},
		stdjson.Format{},
		protobuf.Format{},
	}
}
