package ocf

import "github.com/iskorotkov/avro/v2"

// minWireSize returns the fewest bytes a value of the schema can occupy in binary encoding.
func minWireSize(schema avro.Schema) int {
	return minWireSizeOf(schema, map[string]bool{})
}

func minWireSizeOf(schema avro.Schema, visiting map[string]bool) int {
	switch s := schema.(type) {
	case *avro.NullSchema:
		return 0
	case *avro.FixedSchema:
		return s.Size()
	case *avro.RefSchema:
		return minWireSizeOf(s.Schema(), visiting)
	case *avro.RecordSchema:
		// A record that contains itself directly can never be encoded, so zero is a safe lower bound.
		if visiting[s.FullName()] {
			return 0
		}
		visiting[s.FullName()] = true
		defer delete(visiting, s.FullName())

		size := 0
		for _, f := range s.Fields() {
			size += minWireSizeOf(f.Type(), visiting)
		}
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
