package avro_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/iskorotkov/avro/v2"
)

func BenchmarkEnumEncode(b *testing.B) {
	// Encoding scans the symbol list, so cost depends on the symbol's index.
	for _, tt := range []struct{ symbols, index int }{{3, 0}, {3, 2}, {100, 0}, {100, 99}} {
		schema := enumSchema(tt.symbols)
		in := "s" + strconv.Itoa(tt.index)

		b.Run(fmt.Sprintf("symbols=%d/index=%d", tt.symbols, tt.index), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := avro.Marshal(schema, in); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func enumSchema(n int) avro.Schema {
	symbols := make([]string, n)
	for i := range symbols {
		symbols[i] = `"s` + strconv.Itoa(i) + `"`
	}
	return avro.MustParse(`{"type":"enum","name":"e","symbols":[` + strings.Join(symbols, ",") + `]}`)
}
