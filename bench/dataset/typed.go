package dataset

import "fmt"

// Typed returns the records of ds as *T, failing when ds holds another type.
func Typed[T any](ds Dataset) ([]*T, error) {
	records := ds.Records()
	out := make([]*T, len(records))
	for i, r := range records {
		v, ok := r.(*T)
		if !ok {
			return nil, fmt.Errorf("dataset %s: record %d is %T, want %T", ds.Name(), i, r, v)
		}
		out[i] = v
	}

	return out, nil
}
