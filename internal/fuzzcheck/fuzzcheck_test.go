package fuzzcheck_test

import (
	"errors"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/internal/fuzzcheck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecode(t *testing.T) {
	schema, err := avro.Parse(`{"type":"array","items":"long"}`)
	require.NoError(t, err)

	tests := []struct {
		name    string
		data    []byte
		want    any
		wantErr bool
	}{
		{name: "valid", data: []byte{0x02, 0x06, 0x00}, want: []any{int64(3)}},
		{name: "empty", data: []byte{}, wantErr: true},
		{name: "ends inside a varint", data: []byte{0x02, 0x86}, wantErr: true},
		{name: "missing end block", data: []byte{0x02, 0x06}, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := fuzzcheck.Decode(avro.DefaultConfig, schema, test.data)

			if test.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestSame(t *testing.T) {
	ts := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		a, b any
		want bool
	}{
		{name: "nil", a: nil, b: nil, want: true},
		{name: "nil and value", a: nil, b: 1, want: false},
		{name: "different types", a: int32(1), b: int64(1), want: false},
		{name: "nan", a: math.NaN(), b: math.NaN(), want: true},
		{name: "nan and number", a: math.NaN(), b: 1.0, want: false},
		{name: "negative zero", a: math.Copysign(0, -1), b: 0.0, want: false},
		{name: "float32", a: float32(1.5), b: float32(1.5), want: true},
		{name: "nested nan", a: map[string]any{"f": []any{float32(math.NaN())}}, b: map[string]any{"f": []any{float32(math.NaN())}}, want: true},
		{name: "map missing key", a: map[string]any{"a": 1}, b: map[string]any{"b": 1}, want: false},
		{name: "map length", a: map[string]any{"a": 1}, b: map[string]any{}, want: false},
		{name: "slice length", a: []any{1}, b: []any{1, 2}, want: false},
		{name: "slice element", a: []any{1}, b: []any{2}, want: false},
		{name: "array", a: [2]byte{1, 2}, b: [2]byte{1, 2}, want: true},
		{name: "nested pointer", a: map[string]any{"d": big.NewRat(1, 2)}, b: map[string]any{"d": big.NewRat(1, 2)}, want: true},
		{name: "nested pointer value", a: map[string]any{"d": big.NewRat(1, 2)}, b: map[string]any{"d": big.NewRat(1, 3)}, want: false},
		{name: "nil pointer", a: (*big.Rat)(nil), b: big.NewRat(1, 2), want: false},
		{name: "time", a: ts, b: ts, want: true},
		{name: "struct field", a: struct{ A int }{1}, b: struct{ A int }{2}, want: false},
		{name: "string", a: "x", b: "x", want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, fuzzcheck.Same(test.a, test.b))
		})
	}
}

// fakeTB stops a RoundTrip check at its first failure.
type fakeTB struct {
	testing.TB

	failed bool
}

func (f *fakeTB) Helper() {}

func (f *fakeTB) Fatalf(string, ...any) { f.failed = true }

func TestRoundTrip(t *testing.T) {
	identity := func(b []byte) (any, error) { return string(b), nil }
	encode := func(v any) ([]byte, error) { return []byte(v.(string)), nil }
	errBad := errors.New("bad")

	tests := []struct {
		name       string
		decode     func([]byte) (any, error)
		encode     func(any) ([]byte, error)
		wantFailed bool
	}{
		{name: "stable", decode: identity, encode: encode},
		{name: "undecodable input is skipped", decode: func([]byte) (any, error) { return nil, errBad }, encode: encode},
		{name: "encode fails", decode: identity, encode: func(any) ([]byte, error) { return nil, errBad }, wantFailed: true},
		{
			name: "second decode fails",
			decode: func(b []byte) (any, error) {
				if string(b) == "x!" {
					return nil, errBad
				}
				return string(b), nil
			},
			encode:     func(v any) ([]byte, error) { return []byte(v.(string) + "!"), nil },
			wantFailed: true,
		},
		{name: "value changes", decode: identity, encode: func(v any) ([]byte, error) { return []byte(v.(string) + "?"), nil }, wantFailed: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tb := &fakeTB{}
			fuzzcheck.RoundTrip(tb, []byte("x"), test.decode, test.encode)

			assert.Equal(t, test.wantFailed, tb.failed)
		})
	}
}
