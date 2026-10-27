// Package datasets lists every benchmark dataset.
package datasets

import (
	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/datasets/earthquakes"
	"github.com/iskorotkov/avro/bench/datasets/nyctaxi"
	"github.com/iskorotkov/avro/bench/datasets/superhero"
)

// All returns the providers of every dataset in benchmark order.
func All() []dataset.Provider {
	return []dataset.Provider{
		superhero.Provider{},
		nyctaxi.Provider{},
		earthquakes.Provider{},
	}
}
