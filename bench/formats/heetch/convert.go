package heetch

import (
	"github.com/iskorotkov/avro/bench/datasets/nyctaxi"
	"github.com/iskorotkov/avro/bench/datasets/superhero"
	gennyctaxi "github.com/iskorotkov/avro/bench/formats/heetch/nyctaxi"
	gensuperhero "github.com/iskorotkov/avro/bench/formats/heetch/superhero"
)

func fromSuperhero(h *superhero.Superhero) gensuperhero.Superhero {
	powers := make([]gensuperhero.Superpower, len(h.Powers))
	for i, p := range h.Powers {
		powers[i] = gensuperhero.Superpower{
			Id:      int(p.ID),
			Name:    p.Name,
			Damage:  p.Damage,
			Energy:  p.Energy,
			Passive: p.Passive,
		}
	}

	return gensuperhero.Superhero{
		Id:             int(h.ID),
		Affiliation_id: int(h.AffiliationID),
		Name:           h.Name,
		Life:           h.Life,
		Energy:         h.Energy,
		Powers:         powers,
	}
}

func fromTrip(t *nyctaxi.Trip) gennyctaxi.Trip {
	return gennyctaxi.Trip{
		Vendor_id:             int(t.VendorID),
		Pickup_time:           t.PickupTime,
		Dropoff_time:          t.DropoffTime,
		Passenger_count:       t.PassengerCount,
		Trip_distance:         t.TripDistance,
		Ratecode_id:           t.RatecodeID,
		Store_and_fwd_flag:    t.StoreAndFwdFlag,
		Pickup_location_id:    int(t.PickupLocationID),
		Dropoff_location_id:   int(t.DropoffLocationID),
		Payment_type:          t.PaymentType,
		Fare_amount:           t.FareAmount,
		Extra:                 t.Extra,
		Mta_tax:               t.MTATax,
		Tip_amount:            t.TipAmount,
		Tolls_amount:          t.TollsAmount,
		Improvement_surcharge: t.ImprovementSurcharge,
		Total_amount:          t.TotalAmount,
		Congestion_surcharge:  t.CongestionSurcharge,
		Airport_fee:           t.AirportFee,
	}
}
