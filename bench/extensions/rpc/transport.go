package rpc

import (
	"context"
	"net"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20

// conn is an in-process client connection to an echo server that counts message bytes.
type conn struct {
	*grpc.ClientConn
	server *grpc.Server
	bytes  payloadCounter
}

func dial(codec Codec, newValue func() any) (*conn, error) {
	lis := bufconn.Listen(bufSize)
	server := grpc.NewServer(codec.server)
	server.RegisterService(&serviceDesc, &echo{newValue: newValue})
	go func() { _ = server.Serve(lis) }()

	c := &conn{server: server}
	cc, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(&c.bytes),
		grpc.WithDefaultCallOptions(codec.call),
	)
	if err != nil {
		server.Stop()
		return nil, err
	}
	c.ClientConn = cc

	return c, nil
}

func (c *conn) Close() {
	_ = c.ClientConn.Close()
	c.server.Stop()
}

// payloadCounter sums the wire length of every message the client sends and receives.
type payloadCounter struct {
	n atomic.Int64
}

func (p *payloadCounter) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }

func (p *payloadCounter) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (p *payloadCounter) HandleConn(context.Context, stats.ConnStats) {}

func (p *payloadCounter) HandleRPC(_ context.Context, s stats.RPCStats) {
	switch s := s.(type) {
	case *stats.OutPayload:
		p.n.Add(int64(s.WireLength))
	case *stats.InPayload:
		p.n.Add(int64(s.WireLength))
	}
}
