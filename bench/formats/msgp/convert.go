package msgp

import (
	"github.com/iskorotkov/avro/bench/datasets/nyctaxi"
	"github.com/iskorotkov/avro/bench/datasets/superhero"
)

func fromSuperhero(h *superhero.Superhero) *Superhero {
	powers := make([]*Superpower, len(h.Powers))
	for i, p := range h.Powers {
		powers[i] = &Superpower{ID: p.ID, Name: p.Name, Damage: p.Damage, Energy: p.Energy, Passive: p.Passive}
	}

	return &Superhero{
		ID:            h.ID,
		AffiliationID: h.AffiliationID,
		Name:          h.Name,
		Life:          h.Life,
		Energy:        h.Energy,
		Powers:        powers,
	}
}

func fromTrip(t *nyctaxi.Trip) *Trip {
	return &Trip{
		VendorID:             t.VendorID,
		PickupTime:           t.PickupTime,
		DropoffTime:          t.DropoffTime,
		PassengerCount:       t.PassengerCount,
		TripDistance:         t.TripDistance,
		RatecodeID:           t.RatecodeID,
		StoreAndFwdFlag:      t.StoreAndFwdFlag,
		PickupLocationID:     t.PickupLocationID,
		DropoffLocationID:    t.DropoffLocationID,
		PaymentType:          t.PaymentType,
		FareAmount:           t.FareAmount,
		Extra:                t.Extra,
		MTATax:               t.MTATax,
		TipAmount:            t.TipAmount,
		TollsAmount:          t.TollsAmount,
		ImprovementSurcharge: t.ImprovementSurcharge,
		TotalAmount:          t.TotalAmount,
		CongestionSurcharge:  t.CongestionSurcharge,
		AirportFee:           t.AirportFee,
	}
}
