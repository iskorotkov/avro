package avro_test

import (
	"testing"

	"github.com/iskorotkov/avro/v2"
)

func FuzzSchemaParse(f *testing.F) {
	defer ConfigTeardown()

	seeds := []string{
		`"null"`,
		`"int"`,
		`"string"`,
		`{"type":"int"}`,
		`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
		`{"type":"long","logicalType":"timestamp-micros"}`,
		`{"type":"fixed","name":"F","size":12,"logicalType":"duration"}`,
		`{"type":"enum","name":"E","symbols":["A","B","C"],"default":"A"}`,
		`{"type":"array","items":"long"}`,
		`{"type":"map","values":"string"}`,
		`["null","int","string"]`,
		`{"type":"record","name":"R","fields":[
			{"name":"a","type":"long"},
			{"name":"b","type":["null","string"],"default":null},
			{"name":"c","type":{"type":"array","items":"int"},"default":[]}
		]}`,
		`{"type":"record","name":"Tree","fields":[
			{"name":"v","type":"int"},
			{"name":"next","type":["null","Tree"],"default":null}
		]}`,
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Add([]byte(""))
	f.Add([]byte("{"))

	f.Fuzz(func(_ *testing.T, data []byte) {
		if len(data) > 1<<24 {
			return
		}
		_, _ = avro.ParseBytes(data)
	})
}
