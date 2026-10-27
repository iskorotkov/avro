package superhero

import (
	"fmt"
	"math/rand/v2"
)

// Generate returns n deterministic pseudo-random superheroes.
func Generate(n int) []Superhero {
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // Deterministic test data.
	heroes := make([]Superhero, n)
	for i := range heroes {
		heroes[i] = Superhero{
			ID:            int32(i + 1),
			AffiliationID: rng.Int32N(50),
			Name:          fmt.Sprintf("Superhero %d", i+1),
			Life:          rng.Float32() * 100,
			Energy:        rng.Float32() * 100,
			Powers:        powers(rng, 1+rng.IntN(10)),
		}
	}

	return heroes
}

func powers(rng *rand.Rand, n int) []*Superpower {
	ps := make([]*Superpower, n)
	for i := range ps {
		ps[i] = &Superpower{
			ID:      rng.Int32N(1000),
			Name:    fmt.Sprintf("Superpower %d", rng.IntN(1000)),
			Damage:  rng.Float32() * 10,
			Energy:  rng.Float32() * 10,
			Passive: rng.IntN(2) == 0,
		}
	}

	return ps
}
