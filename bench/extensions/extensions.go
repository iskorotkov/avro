// Package extensions lists every extension that adds operations to the benchmark suite.
package extensions

import (
	"github.com/iskorotkov/avro/bench/extensions/rpc"
	"github.com/iskorotkov/avro/bench/formats/protobuf"
	"github.com/iskorotkov/avro/bench/formats/vtprotobuf"
	"github.com/iskorotkov/avro/bench/suite"
	vtgrpc "github.com/planetscale/vtprotobuf/codec/grpc"
	"google.golang.org/grpc/encoding"
	grpcproto "google.golang.org/grpc/encoding/proto"
)

// All returns every extension.
func All() []suite.Extension {
	return []suite.Extension{
		rpc.Extension{Codecs: map[string]rpc.Codec{
			protobuf.Name:   rpc.V2(encoding.GetCodecV2(grpcproto.Name)),
			vtprotobuf.Name: rpc.V1(vtgrpc.Codec{}),
		}, Records: 1000},
	}
}
