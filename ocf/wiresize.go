package ocf

import (
	"math"

	"github.com/iskorotkov/avro/v2"
)

// minWireSize returns the fewest bytes a value of the schema can occupy in binary encoding.
func minWireSize(schema avro.Schema) int {
	w := wireSizer{memo: map[string]int{}}
	return w.size(schema)
}

// wireSizer memoizes records by name; a record seen mid-walk counts as zero, which is still a lower bound.
type wireSizer struct {
	memo   map[string]int
	visits int
}

func (w *wireSizer) size(schema avro.Schema) int {
	w.visits++
	switch s := schema.(type) {
	case *avro.NullSchema:
		return 0
	case *avro.FixedSchema:
		return min(s.Size(), math.MaxInt32)
	case *avro.RefSchema:
		return w.size(s.Schema())
	case *avro.RecordSchema:
		if size, ok := w.memo[s.FullName()]; ok {
			return size
		}
		w.memo[s.FullName()] = 0

		size := 0
		for _, f := range s.Fields() {
			size = saturatingAdd(size, w.size(f.Type()))
		}
		w.memo[s.FullName()] = size
		return size
	}

	switch schema.Type() {
	case avro.Float:
		return 4
	case avro.Double:
		return 8
	default:
		return 1
	}
}

// saturatingAdd adds two sizes in [0, MaxInt32] without overflowing a 32-bit int.
func saturatingAdd(a, b int) int {
	if b > math.MaxInt32-a {
		return math.MaxInt32
	}
	return a + b
}
