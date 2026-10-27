package extensions_test

import (
	"testing"

	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/datasets/superhero"
	"github.com/iskorotkov/avro/bench/extensions"
	"github.com/iskorotkov/avro/bench/formats"
	"github.com/iskorotkov/avro/bench/suite"
)

func TestAllRoundTripEveryFormat(t *testing.T) {
	cases, err := suite.Load("", []dataset.Provider{superhero.Provider{}}, formats.All())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.Format, func(t *testing.T) {
			for _, e := range extensions.All() {
				if err := e.Verify(c); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
