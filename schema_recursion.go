package avro

// isRecursive reports whether a value of rec can contain another value of rec.
func isRecursive(rec *RecordSchema) bool {
	seen := map[*RecordSchema]bool{rec: true}
	for _, f := range rec.Fields() {
		if reaches(f.Type(), rec, seen) {
			return true
		}
	}
	return false
}

// reaches reports whether schema can contain target, visiting each record once.
func reaches(schema Schema, target *RecordSchema, seen map[*RecordSchema]bool) bool {
	switch s := schema.(type) {
	case *RefSchema:
		return reaches(s.Schema(), target, seen)
	case *ArraySchema:
		return reaches(s.Items(), target, seen)
	case *MapSchema:
		return reaches(s.Values(), target, seen)
	case *UnionSchema:
		return reachesAny(s.Types(), target, seen)
	case *RecordSchema:
		return reachesRecord(s, target, seen)
	default:
		return false
	}
}

func reachesRecord(rec, target *RecordSchema, seen map[*RecordSchema]bool) bool {
	if rec == target {
		return true
	}
	if seen[rec] {
		return false
	}
	seen[rec] = true

	for _, f := range rec.Fields() {
		if reaches(f.Type(), target, seen) {
			return true
		}
	}
	return false
}

func reachesAny(types []Schema, target *RecordSchema, seen map[*RecordSchema]bool) bool {
	for _, t := range types {
		if reaches(t, target, seen) {
			return true
		}
	}
	return false
}
