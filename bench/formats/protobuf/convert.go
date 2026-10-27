package protobuf

import (
	"github.com/iskorotkov/avro/bench/datasets/nyctaxi"
	"github.com/iskorotkov/avro/bench/datasets/superhero"
)

func fromSuperhero(h *superhero.Superhero) *Superhero {
	powers := make([]*Superpower, len(h.Powers))
	for i, p := range h.Powers {
		powers[i] = &Superpower{Id: p.ID, Name: p.Name, Damage: p.Damage, Energy: p.Energy, Passive: p.Passive}
	}

	return &Superhero{
		Id:            h.ID,
		AffiliationId: h.AffiliationID,
		Name:          h.Name,
		Life:          h.Life,
		Energy:        h.Energy,
		Powers:        powers,
	}
}

func fromTrip(t *nyctaxi.Trip) *Trip {
	return &Trip{
		VendorId:             t.VendorID,
		PickupTime:           t.PickupTime.UnixMicro(),
		DropoffTime:          t.DropoffTime.UnixMicro(),
		PassengerCount:       t.PassengerCount,
		TripDistance:         t.TripDistance,
		RatecodeId:           t.RatecodeID,
		StoreAndFwdFlag:      t.StoreAndFwdFlag,
		PickupLocationId:     t.PickupLocationID,
		DropoffLocationId:    t.DropoffLocationID,
		PaymentType:          t.PaymentType,
		FareAmount:           t.FareAmount,
		Extra:                t.Extra,
		MtaTax:               t.MTATax,
		TipAmount:            t.TipAmount,
		TollsAmount:          t.TollsAmount,
		ImprovementSurcharge: t.ImprovementSurcharge,
		TotalAmount:          t.TotalAmount,
		CongestionSurcharge:  t.CongestionSurcharge,
		AirportFee:           t.AirportFee,
	}
}
