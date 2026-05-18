package avro_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/iskorotkov/avro/v2"
)

type benchTextKey int

func (k *benchTextKey) UnmarshalText(data []byte) error {
	i, err := strconv.Atoi(string(data))
	if err != nil {
		return err
	}
	*k = benchTextKey(i)
	return nil
}

func (k *benchTextKey) MarshalText() ([]byte, error) {
	return strconv.AppendInt(nil, int64(*k), 10), nil
}

func benchMakeMarshalerMap(n int) map[*benchTextKey]int64 {
	out := make(map[*benchTextKey]int64, n)
	for i := range n {
		k := benchTextKey(i)
		out[&k] = int64(i)
	}
	return out
}

func BenchmarkMapTextUnmarshalerDecode(b *testing.B) {
	schema, err := avro.Parse(`{"type":"map","values":"long"}`)
	if err != nil {
		b.Fatal(err)
	}

	for _, n := range []int{10, 100, 1000} {
		data, err := avro.Marshal(schema, benchMakeMarshalerMap(n))
		if err != nil {
			b.Fatal(err)
		}

		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var got map[*benchTextKey]int64
				if err := avro.Unmarshal(schema, data, &got); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMapTextMarshalerEncode(b *testing.B) {
	schema, err := avro.Parse(`{"type":"map","values":"long"}`)
	if err != nil {
		b.Fatal(err)
	}

	for _, n := range []int{10, 100, 1000} {
		in := benchMakeMarshalerMap(n)

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

func benchMakeTimeMap(n int) map[time.Time]int64 {
	out := make(map[time.Time]int64, n)
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range n {
		out[base.Add(time.Duration(i)*time.Second)] = int64(i)
	}
	return out
}

func BenchmarkMapTimeKeyEncode(b *testing.B) {
	schema, err := avro.Parse(`{"type":"map","values":"long"}`)
	if err != nil {
		b.Fatal(err)
	}

	for _, n := range []int{10, 100, 1000} {
		in := benchMakeTimeMap(n)

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
