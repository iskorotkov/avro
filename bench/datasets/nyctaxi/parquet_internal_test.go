package nyctaxi

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestReadParquetStrideAndRows(t *testing.T) {
	rows := make([]row, 10)
	for i := range rows {
		id := int32(i)
		rows[i].VendorID = &id
	}
	path := filepath.Join(t.TempDir(), "trips.parquet")
	if err := parquet.WriteFile(path, rows); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		stride, rows int
		want         []int32
	}{
		{1, 0, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{3, 0, []int32{0, 3, 6, 9}},
		{3, 2, []int32{0, 3}},
		{1, 20, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
	} {
		trips, err := ReadParquet(path, tc.stride, tc.rows)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]int32, len(trips))
		for i, trip := range trips {
			got[i] = trip.VendorID
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("stride %d rows %d: got %v, want %v", tc.stride, tc.rows, got, tc.want)
		}
	}
}
