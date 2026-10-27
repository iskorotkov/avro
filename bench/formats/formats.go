// Package formats lists every compared serialisation format.
package formats

import (
	"github.com/iskorotkov/avro/bench/format"
	"github.com/iskorotkov/avro/bench/formats/avro"
	"github.com/iskorotkov/avro/bench/formats/cbor"
	"github.com/iskorotkov/avro/bench/formats/goccy"
	"github.com/iskorotkov/avro/bench/formats/hamba"
	"github.com/iskorotkov/avro/bench/formats/jsonexp"
	"github.com/iskorotkov/avro/bench/formats/msgp"
	"github.com/iskorotkov/avro/bench/formats/msgpack"
	"github.com/iskorotkov/avro/bench/formats/protobuf"
	"github.com/iskorotkov/avro/bench/formats/sonic"
	"github.com/iskorotkov/avro/bench/formats/stdjson"
)

// All returns every format, the baseline first, followed by build-tagged formats.
func All() []format.Format {
	return append([]format.Format{
		avro.Format{},
		hamba.Format{},
		stdjson.Format{},
		jsonexp.Format{},
		goccy.Format{},
		sonic.Format{},
		protobuf.Format{},
		msgp.Format{},
		msgpack.Format{},
		cbor.Format{},
	}, tagged()...)
}
