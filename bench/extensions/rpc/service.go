package rpc

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc"
)

const (
	unaryMethod  = "/bench.Echo/Unary"
	streamMethod = "/bench.Echo/Stream"
)

// echo returns every received value to the client, decoding and re-encoding it.
type echo struct {
	newValue func() any
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: "bench.Echo",
	HandlerType: (*any)(nil),
	Methods:     []grpc.MethodDesc{{MethodName: "Unary", Handler: unaryHandler}},
	Streams:     []grpc.StreamDesc{streamDesc},
}

var streamDesc = grpc.StreamDesc{StreamName: "Stream", Handler: streamHandler, ServerStreams: true, ClientStreams: true}

// unaryHandler skips the interceptor because the benchmark server installs none.
func unaryHandler(srv any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	v := srv.(*echo).newValue()
	if err := dec(v); err != nil {
		return nil, err
	}

	return v, nil
}

func streamHandler(srv any, stream grpc.ServerStream) error {
	for {
		v := srv.(*echo).newValue()
		err := stream.RecvMsg(v)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.SendMsg(v); err != nil {
			return err
		}
	}
}
