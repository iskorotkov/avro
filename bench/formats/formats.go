// Package formats lists every compared serialisation format.
package formats

import (
	"github.com/iskorotkov/avro/bench/format"
	"github.com/iskorotkov/avro/bench/formats/avro"
	"github.com/iskorotkov/avro/bench/formats/avrogen"
	"github.com/iskorotkov/avro/bench/formats/goavro"
	"github.com/iskorotkov/avro/bench/formats/gogenavro"
	"github.com/iskorotkov/avro/bench/formats/hamba"
	"github.com/iskorotkov/avro/bench/formats/protobuf"
	"github.com/iskorotkov/avro/bench/formats/stdjson"
	"github.com/iskorotkov/avro/bench/formats/twmb"
)

// All returns every format, the baseline first.
func All() []format.Format {
	return []format.Format{
		avro.Format{},
		avrogen.Format{},
		hamba.Format{},
		twmb.Format{},
		goavro.Format{},
		gogenavro.Format{},
		stdjson.Format{},
		protobuf.Format{},
	}
}
