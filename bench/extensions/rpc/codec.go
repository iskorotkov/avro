package rpc

import (
	"github.com/iskorotkov/avro/bench/format"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

// Codec is the gRPC codec forced on both ends of a connection.
type Codec struct {
	server grpc.ServerOption
	call   grpc.CallOption
}

// V1 forces a byte slice based gRPC codec.
func V1(c encoding.Codec) Codec {
	return Codec{server: grpc.ForceServerCodec(c), call: grpc.ForceCodec(c)}
}

// V2 forces a buffer pool based gRPC codec, such as the default protobuf one.
func V2(c encoding.CodecV2) Codec {
	return Codec{server: grpc.ForceServerCodecV2(c), call: grpc.ForceCodecV2(c)}
}

// Adapt wraps a format codec as a gRPC codec.
func Adapt(c format.Codec) encoding.Codec { return adapter{c} }

type adapter struct {
	codec format.Codec
}

func (a adapter) Marshal(v any) ([]byte, error) { return a.codec.Marshal(v) }

func (a adapter) Unmarshal(data []byte, v any) error { return a.codec.Unmarshal(data, v) }

func (adapter) Name() string { return "bench" }
