package formats_test

import (
	"errors"
	"testing"
	"time"

	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/datasets/nyctaxi"
	"github.com/iskorotkov/avro/bench/format"
	"github.com/iskorotkov/avro/bench/formats"
)

// The committed samples have whole-second times, so fractions are checked here.
func TestSubSecondTimestamps(t *testing.T) {
	at := time.Date(2024, 1, 2, 3, 4, 5, 15838000, time.UTC)
	sample, err := nyctaxi.Provider{}.Load("")
	if err != nil {
		t.Fatal(err)
	}
	ds := dataset.New(nyctaxi.Name, sample.Schema(), []nyctaxi.Trip{{PickupTime: at, DropoffTime: at.Add(time.Microsecond)}})

	for _, f := range formats.All() {
		t.Run(f.Name(), func(t *testing.T) {
			s, err := f.Bind(ds)
			if errors.Is(err, format.ErrUnsupported) {
				t.Skip(err)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := format.Verify(s); err != nil {
				t.Fatal(err)
			}
		})
	}
}
