package avro

// maxWireSize saturates minimum wire sizes so sums over huge schemas cannot overflow.
const maxWireSize = 1 << 30

// minWireSize returns a lower bound on the number of bytes any value of schema occupies on the wire.
func minWireSize(schema Schema) int {
	return newWireSizer().size(schema)
}

// wireSizer memoizes sizes per record; a value of -1 marks a record that is still being sized.
type wireSizer struct {
	sizes map[*RecordSchema]int
	// visits counts records sized without the memo.
	visits int
}

func newWireSizer() *wireSizer {
	return &wireSizer{sizes: map[*RecordSchema]int{}}
}

func (ws *wireSizer) size(schema Schema) int {
	switch s := schema.(type) {
	case *PrimitiveSchema:
		return primitiveWireSize(s.Type())
	case *EnumSchema, *ArraySchema, *MapSchema:
		return 1
	case *FixedSchema:
		return min(s.Size(), maxWireSize)
	case *RefSchema:
		return ws.size(s.Schema())
	case *UnionSchema:
		return min(1+ws.minOf(s.Types()), maxWireSize)
	case *RecordSchema:
		return ws.record(s)
	default:
		return 0
	}
}

func (ws *wireSizer) record(s *RecordSchema) int {
	if n, ok := ws.sizes[s]; ok {
		// A record reached again while being sized contributes nothing to the lower bound.
		return max(n, 0)
	}
	ws.sizes[s] = -1
	ws.visits++

	total := 0
	for _, f := range s.Fields() {
		total = saturatingAdd(total, ws.size(f.Type()))
	}
	ws.sizes[s] = total
	return total
}

// saturatingAdd adds sizes of at most maxWireSize without overflowing a 32-bit int.
func saturatingAdd(a, b int) int {
	if b > maxWireSize-a {
		return maxWireSize
	}
	return a + b
}

func (ws *wireSizer) minOf(types []Schema) int {
	smallest := maxWireSize
	for _, t := range types {
		smallest = min(smallest, ws.size(t))
	}
	return smallest
}

func primitiveWireSize(typ Type) int {
	switch typ {
	case Null:
		return 0
	case Float:
		return 4
	case Double:
		return 8
	default:
		return 1
	}
}
