package avro

import "slices"

// isRecursive reports whether a value of rec can contain another value of rec.
func isRecursive(rec *RecordSchema) bool {
	return newRecursionFinder(rec).fromRecord(rec)
}

// recursionFinder searches schemas for its target record, visiting each other record once.
type recursionFinder struct {
	target *RecordSchema
	seen   map[*RecordSchema]bool
	// visits counts records whose fields were searched.
	visits int
}

func newRecursionFinder(target *RecordSchema) *recursionFinder {
	return &recursionFinder{target: target, seen: map[*RecordSchema]bool{target: true}}
}

func (f *recursionFinder) fromRecord(rec *RecordSchema) bool {
	f.visits++
	for _, field := range rec.Fields() {
		if f.reaches(field.Type()) {
			return true
		}
	}
	return false
}

// reaches reports whether schema can contain the target.
func (f *recursionFinder) reaches(schema Schema) bool {
	switch s := schema.(type) {
	case *RefSchema:
		return f.reaches(s.Schema())
	case *ArraySchema:
		return f.reaches(s.Items())
	case *MapSchema:
		return f.reaches(s.Values())
	case *UnionSchema:
		return slices.ContainsFunc(s.Types(), f.reaches)
	case *RecordSchema:
		return f.reachesRecord(s)
	default:
		return false
	}
}

func (f *recursionFinder) reachesRecord(rec *RecordSchema) bool {
	if rec == f.target {
		return true
	}
	if f.seen[rec] {
		return false
	}
	f.seen[rec] = true
	return f.fromRecord(rec)
}
