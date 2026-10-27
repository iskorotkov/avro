package dataset

import "slices"

// Head returns a copy of the first rows records, or all of them when rows is not positive.
func Head[T any](records []T, rows int) []T {
	if rows <= 0 || rows >= len(records) {
		return records
	}

	return slices.Clone(records[:rows])
}
