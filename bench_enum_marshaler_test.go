package avro_test

import (
	"errors"
	"strconv"
	"testing"

	"github.com/iskorotkov/avro/v2"
)

type benchEnumTextMarshaler int

func (m *benchEnumTextMarshaler) MarshalText() ([]byte, error) {
	switch *m {
	case 0:
		return []byte("foo"), nil
	case 1:
		return []byte("bar"), nil
	case 2:
		return []byte("baz"), nil
	}
	return nil, errors.New("unknown")
}

func BenchmarkEnumArrayTextMarshalerEncode(b *testing.B) {
	schema, err := avro.Parse(`{"type":"array","items":{"type":"enum","name":"e","symbols":["foo","bar","baz"]}}`)
	if err != nil {
		b.Fatal(err)
	}

	for _, n := range []int{10, 100, 1000} {
		in := make([]benchEnumTextMarshaler, n)
		for i := range in {
			in[i] = benchEnumTextMarshaler(i % 3)
		}

		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := avro.Marshal(schema, in); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
