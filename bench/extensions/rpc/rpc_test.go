package rpc_test

import (
	"errors"
	"testing"
	"time"

	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/extensions/rpc"
	"github.com/iskorotkov/avro/bench/format"
	"github.com/iskorotkov/avro/bench/formats/stdjson"
	"github.com/iskorotkov/avro/bench/suite"
)

type record struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func jsonCase(tb testing.TB, n int) suite.Case {
	tb.Helper()
	records := make([]record, n)
	for i := range records {
		records[i] = record{ID: int64(i), Name: "r"}
	}
	s, err := stdjson.Format{}.Bind(dataset.New("toy", "", records))
	if err != nil {
		tb.Fatal(err)
	}

	return suite.Case{Dataset: "toy", Format: stdjson.Name, Subject: s}
}

// dropName loses a field on decode so the round trip check must fail.
type dropName struct {
	format.Codec
}

func (d dropName) Unmarshal(data []byte, v any) error {
	err := d.Codec.Unmarshal(data, v)
	v.(*record).Name = ""

	return err
}

func TestVerify(t *testing.T) {
	if err := (rpc.Extension{}).Verify(jsonCase(t, 50)); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyDetectsLossyCodec(t *testing.T) {
	c := jsonCase(t, 3)
	ext := rpc.Extension{Codecs: map[string]rpc.Codec{stdjson.Name: rpc.V1(rpc.Adapt(dropName{c.Subject.Codec}))}}
	if err := ext.Verify(c); err == nil {
		t.Fatal("lossy codec passed verification")
	}
}

// failMarshal cannot encode, so every RPC must fail without hanging.
type failMarshal struct {
	format.Codec
}

func (failMarshal) Marshal(any) ([]byte, error) { return nil, errors.New("marshal failed") }

func TestVerifyFailsFastOnMarshalError(t *testing.T) {
	c := jsonCase(t, 3)
	ext := rpc.Extension{Codecs: map[string]rpc.Codec{stdjson.Name: rpc.V1(rpc.Adapt(failMarshal{c.Subject.Codec}))}}
	done := make(chan error, 1)
	go func() { done <- ext.Verify(c) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("failing codec passed verification")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("verification hung")
	}
}

func BenchmarkOps(b *testing.B) {
	c := jsonCase(b, 20)
	for _, op := range (rpc.Extension{Records: 10}).Ops(c) {
		b.Run(op.Name, func(b *testing.B) { op.Run(b, c.Subject) })
	}
}
