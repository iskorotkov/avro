package goavro

import (
	"github.com/iskorotkov/avro/bench/datasets/nyctaxi"
	"github.com/iskorotkov/avro/bench/datasets/superhero"
)

func fromSuperhero(h *superhero.Superhero) Record {
	powers := make([]any, len(h.Powers))
	for i, p := range h.Powers {
		powers[i] = Record{
			"id":      p.ID,
			"name":    p.Name,
			"damage":  p.Damage,
			"energy":  p.Energy,
			"passive": p.Passive,
		}
	}

	return Record{
		"id":             h.ID,
		"affiliation_id": h.AffiliationID,
		"name":           h.Name,
		"life":           h.Life,
		"energy":         h.Energy,
		"powers":         powers,
	}
}

func fromTrip(t *nyctaxi.Trip) Record {
	return Record{
		"vendor_id":             t.VendorID,
		"pickup_time":           t.PickupTime,
		"dropoff_time":          t.DropoffTime,
		"passenger_count":       union("long", t.PassengerCount),
		"trip_distance":         t.TripDistance,
		"ratecode_id":           union("long", t.RatecodeID),
		"store_and_fwd_flag":    union("string", t.StoreAndFwdFlag),
		"pickup_location_id":    t.PickupLocationID,
		"dropoff_location_id":   t.DropoffLocationID,
		"payment_type":          t.PaymentType,
		"fare_amount":           t.FareAmount,
		"extra":                 t.Extra,
		"mta_tax":               t.MTATax,
		"tip_amount":            t.TipAmount,
		"tolls_amount":          t.TollsAmount,
		"improvement_surcharge": t.ImprovementSurcharge,
		"total_amount":          t.TotalAmount,
		"congestion_surcharge":  union("double", t.CongestionSurcharge),
		"airport_fee":           union("double", t.AirportFee),
	}
}

// union wraps a nullable value the way goavro represents union branches.
func union[T any](branch string, p *T) any {
	if p == nil {
		return nil
	}

	return map[string]any{branch: *p}
}
