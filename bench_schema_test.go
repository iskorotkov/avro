package avro_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/iskorotkov/avro/v2"
)

func defaultsSchema(fields int) string {
	var sb strings.Builder
	sb.WriteString(`{"type":"record","name":"Defaults","namespace":"bench.defaults","fields":[`)
	for i := range fields {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"name":"f` + strconv.Itoa(i) + `","type":["null","string"],"default":null}`)
	}
	sb.WriteString(`,{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"n","type":"long","default":42}]},"default":{"n":1}}]}`)
	return sb.String()
}

func BenchmarkCacheFingerprint_WithDefaults(b *testing.B) {
	src := defaultsSchema(20)

	b.Run("Cold", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			schema, err := avro.ParseWithCache(src, "", &avro.SchemaCache{})
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			_ = schema.CacheFingerprint()
		}
	})

	b.Run("Warm", func(b *testing.B) {
		schema, err := avro.ParseWithCache(src, "", &avro.SchemaCache{})
		if err != nil {
			b.Fatal(err)
		}
		_ = schema.CacheFingerprint()
		b.ReportAllocs()
		for b.Loop() {
			_ = schema.CacheFingerprint()
		}
	})
}

func BenchmarkParse_SharedCacheParallel(b *testing.B) {
	src := defaultsSchema(5)
	cache := &avro.SchemaCache{}
	if _, err := avro.ParseWithCache(src, "", cache); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := avro.ParseWithCache(src, "", cache); err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkParse_PrivateCacheParallel(b *testing.B) {
	src := defaultsSchema(5)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := avro.ParseWithCache(src, "", &avro.SchemaCache{}); err != nil {
				b.Error(err)
			}
		}
	})
}
