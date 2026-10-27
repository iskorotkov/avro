package suite

import (
	"bytes"
	"cmp"
	"slices"
	"strings"
	"testing"

	"github.com/iskorotkov/avro/bench/format"
)

// Op is a benchmarked operation over a whole dataset; decodes fill new values like a typical consumer.
type Op struct {
	Name string
	Run  func(b *testing.B, s *format.Subject)
}

// Ops returns the operations a subject supports.
func Ops(s *format.Subject) []Op {
	ops := []Op{{"encode", encode}, {"decode", decode}}
	if s.Batch != nil {
		ops = append(ops, Op{"batch-encode", batchEncode}, Op{"batch-decode", batchDecode})
	}
	if s.Container != nil {
		for _, c := range s.Container.Compressions() {
			ops = append(ops, Op{"container-write-" + c, containerWrite(c)}, Op{"container-read-" + c, containerRead(c)})
		}
	}

	return ops
}

// CompareOps orders op names: single, batch, then container ops by name.
func CompareOps(a, b string) int {
	return cmp.Or(cmp.Compare(rank(a), rank(b)), strings.Compare(a, b))
}

func rank(op string) int {
	order := []string{"encode", "decode", "batch-encode", "batch-decode"}
	if i := slices.Index(order, op); i >= 0 {
		return i
	}

	return len(order)
}

func encode(b *testing.B, s *format.Subject) {
	size := 0
	for _, v := range s.Values {
		data, err := s.Codec.Marshal(v)
		if err != nil {
			b.Fatal(err)
		}
		size += len(data)
	}

	measure(b, len(s.Values), size, func() {
		for _, v := range s.Values {
			if _, err := s.Codec.Marshal(v); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func decode(b *testing.B, s *format.Subject) {
	payloads := make([][]byte, len(s.Values))
	size := 0
	for i, v := range s.Values {
		data, err := s.Codec.Marshal(v)
		if err != nil {
			b.Fatal(err)
		}
		payloads[i] = bytes.Clone(data)
		size += len(data)
	}

	measure(b, len(payloads), size, func() {
		for _, p := range payloads {
			if err := s.Codec.Unmarshal(p, s.New()); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func batchEncode(b *testing.B, s *format.Subject) {
	data, err := s.Batch.MarshalBatch(s.Values)
	if err != nil {
		b.Fatal(err)
	}

	measure(b, len(s.Values), len(data), func() {
		if _, err := s.Batch.MarshalBatch(s.Values); err != nil {
			b.Fatal(err)
		}
	})
}

func batchDecode(b *testing.B, s *format.Subject) {
	data, err := s.Batch.MarshalBatch(s.Values)
	if err != nil {
		b.Fatal(err)
	}
	data = bytes.Clone(data)

	measure(b, len(s.Values), len(data), func() {
		if err := s.Batch.UnmarshalBatch(data, s.NewValues(len(s.Values))); err != nil {
			b.Fatal(err)
		}
	})
}

func containerWrite(compression string) func(*testing.B, *format.Subject) {
	return func(b *testing.B, s *format.Subject) {
		var buf bytes.Buffer
		if err := s.Container.WriteContainer(&buf, compression, s.Values); err != nil {
			b.Fatal(err)
		}

		measure(b, len(s.Values), buf.Len(), func() {
			buf.Reset()
			if err := s.Container.WriteContainer(&buf, compression, s.Values); err != nil {
				b.Fatal(err)
			}
		})
	}
}

func containerRead(compression string) func(*testing.B, *format.Subject) {
	return func(b *testing.B, s *format.Subject) {
		var buf bytes.Buffer
		if err := s.Container.WriteContainer(&buf, compression, s.Values); err != nil {
			b.Fatal(err)
		}
		data := buf.Bytes()

		measure(b, len(s.Values), len(data), func() {
			if err := s.Container.ReadContainer(bytes.NewReader(data), s.NewValues(len(s.Values))); err != nil {
				b.Fatal(err)
			}
		})
	}
}

// measure times run over records and reports throughput and wire size metrics.
func measure(b *testing.B, records, wireBytes int, run func()) {
	b.Helper()
	b.SetBytes(int64(wireBytes))
	b.ReportAllocs()

	for b.Loop() {
		run()
	}

	nsPerRecord := float64(b.Elapsed().Nanoseconds()) / float64(b.N*records)
	b.ReportMetric(nsPerRecord, "ns/rec")
	b.ReportMetric(1e9/nsPerRecord, "rec/s")
	b.ReportMetric(float64(wireBytes)/float64(records), "wire-B/rec")
}
