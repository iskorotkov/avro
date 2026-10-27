package vtprotobuf_test

import (
	"errors"
	"testing"

	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/datasets/superhero"
	"github.com/iskorotkov/avro/bench/format"
	"github.com/iskorotkov/avro/bench/formats/vtprotobuf"
)

func TestBindRoundTrips(t *testing.T) {
	ds, err := superhero.Provider{}.Load("")
	if err != nil {
		t.Fatal(err)
	}
	s, err := vtprotobuf.Format{}.Bind(ds)
	if err != nil {
		t.Fatal(err)
	}
	if err := format.Verify(s); err != nil {
		t.Fatal(err)
	}
}

func TestBindUnsupported(t *testing.T) {
	_, err := vtprotobuf.Format{}.Bind(dataset.New("other", "", []struct{}{{}}))
	if !errors.Is(err, format.ErrUnsupported) {
		t.Errorf("err = %v, want ErrUnsupported", err)
	}
}
