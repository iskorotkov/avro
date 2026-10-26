package avro_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/iskorotkov/avro/v2"
)

func BenchmarkUnionConverterEncoderBuild(b *testing.B) {
	v := 1
	in := &UnionRecord{Int: &v}

	for _, n := range []int{2, 32} {
		schema := nullLastUnionSchema(n)

		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				// A fresh config rebuilds the encoder on every iteration.
				if _, err := (avro.Config{}).Freeze().Marshal(schema, in); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// nullLastUnionSchema puts null last so the null lookup scans every union member.
func nullLastUnionSchema(n int) avro.Schema {
	types := []string{`"int"`}
	for i := range n - 2 {
		types = append(types, `{"type":"fixed","name":"f`+strconv.Itoa(i)+`","size":1}`)
	}
	types = append(types, `"null"`)
	return avro.MustParse("[" + strings.Join(types, ",") + "]")
}
