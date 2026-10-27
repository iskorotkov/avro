package avrogen

import (
	"github.com/iskorotkov/avro/bench/datasets/nyctaxi"
	"github.com/iskorotkov/avro/bench/datasets/superhero"
	gennyctaxi "github.com/iskorotkov/avro/bench/formats/avrogen/nyctaxi"
	gensuperhero "github.com/iskorotkov/avro/bench/formats/avrogen/superhero"
)

func fromSuperhero(h *superhero.Superhero) *gensuperhero.Superhero {
	powers := make([]gensuperhero.Superpower, len(h.Powers))
	for i, p := range h.Powers {
		powers[i] = gensuperhero.Superpower(*p)
	}

	return &gensuperhero.Superhero{
		ID:            h.ID,
		AffiliationID: h.AffiliationID,
		Name:          h.Name,
		Life:          h.Life,
		Energy:        h.Energy,
		Powers:        powers,
	}
}

// fromTrip relies on the generated struct having the same fields as the canonical record.
func fromTrip(t *nyctaxi.Trip) *gennyctaxi.Trip {
	g := gennyctaxi.Trip(*t)
	return &g
}
