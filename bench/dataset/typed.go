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

// Convert maps the canonical records of ds, which must be *T, to native values with f.
func Convert[T, U any](ds Dataset, f func(*T) U) ([]any, error) {
	records, err := Typed[T](ds)
	if err != nil {
		return nil, err
	}

	out := make([]any, len(records))
	for i, r := range records {
		out[i] = f(r)
	}

	return out, nil
}
