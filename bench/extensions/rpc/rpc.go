// Package rpc benchmarks formats as gRPC codecs over an in-process bufconn transport.
package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/iskorotkov/avro/bench/format"
	"github.com/iskorotkov/avro/bench/suite"
	"google.golang.org/grpc"
)

// Op names.
const (
	UnaryOp  = "grpc-unary"
	StreamOp = "grpc-stream"
)

// Extension adds unary and bidirectional streaming echo RPCs to every case.
type Extension struct {
	// Codecs overrides the gRPC codec per format name; other formats use Adapt of their codec.
	Codecs map[string]Codec
	// Records caps the values sent per operation to keep unary runs short; zero sends all.
	Records int
}

// roundTrip sends every value of s through c and returns the echoed values.
type roundTrip func(ctx context.Context, c *conn, s *format.Subject) ([]any, error)

// Ops implements suite.Extension.
func (e Extension) Ops(c suite.Case) []suite.Op {
	codec := e.codec(c)

	return []suite.Op{
		{Name: UnaryOp, Run: e.bench(codec, unary)},
		{Name: StreamOp, Run: e.bench(codec, stream)},
	}
}

// Verify implements suite.Extension.
func (e Extension) Verify(c suite.Case) error {
	codec := e.codec(c)
	for _, op := range []struct {
		name string
		rt   roundTrip
	}{{UnaryOp, unary}, {StreamOp, stream}} {
		conn, err := check(codec, op.rt, e.head(c.Subject))
		if err != nil {
			return fmt.Errorf("%s: %w", op.name, err)
		}
		conn.Close()
	}

	return nil
}

func (e Extension) codec(c suite.Case) Codec {
	if codec, ok := e.Codecs[c.Format]; ok {
		return codec
	}

	return V1(Adapt(c.Subject.Codec))
}

// check verifies one round trip of s on a new connection and returns the connection.
func check(codec Codec, rt roundTrip, s *format.Subject) (*conn, error) {
	c, err := dial(codec, s.New)
	if err != nil {
		return nil, err
	}
	got, err := rt(context.Background(), c, s)
	if err == nil {
		err = s.Check(got)
	}
	if err != nil {
		c.Close()
		return nil, err
	}

	return c, nil
}

// head returns s limited to the first Records values.
func (e Extension) head(s *format.Subject) *format.Subject {
	if e.Records == 0 || e.Records >= len(s.Values) {
		return s
	}
	h := *s
	h.Values = s.Values[:e.Records]

	return &h
}

func (e Extension) bench(codec Codec, rt roundTrip) func(*testing.B, *format.Subject) {
	return func(b *testing.B, s *format.Subject) {
		s = e.head(s)
		c, err := check(codec, rt, s)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(c.Close)

		ctx := b.Context()
		suite.Measure(b, len(s.Values), int(c.bytes.n.Load()), func() {
			if _, err := rt(ctx, c, s); err != nil {
				b.Fatal(err)
			}
		})
	}
}

func unary(ctx context.Context, c *conn, s *format.Subject) ([]any, error) {
	got := make([]any, len(s.Values))
	for i, v := range s.Values {
		got[i] = s.New()
		if err := c.Invoke(ctx, unaryMethod, v, got[i]); err != nil {
			return nil, fmt.Errorf("value %d: %w", i, err)
		}
	}

	return got, nil
}

func stream(ctx context.Context, c *conn, s *format.Subject) ([]any, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	st, err := c.NewStream(ctx, &streamDesc, streamMethod)
	if err != nil {
		return nil, err
	}
	sent := make(chan error, 1)
	go func() { sent <- send(st, s.Values) }()

	got := make([]any, len(s.Values))
	for i := range got {
		got[i] = s.New()
		if err := st.RecvMsg(got[i]); err != nil {
			cancel()
			return nil, errors.Join(fmt.Errorf("receive value %d: %w", i, err), <-sent)
		}
	}
	if err := st.RecvMsg(s.New()); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("stream end: %w", err)
	}

	return got, <-sent
}

func send(st grpc.ClientStream, vs []any) error {
	for i, v := range vs {
		if err := st.SendMsg(v); err != nil {
			return fmt.Errorf("send value %d: %w", i, err)
		}
	}

	return st.CloseSend()
}
