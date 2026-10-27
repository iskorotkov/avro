package gogenavro

import (
	"github.com/iskorotkov/avro/bench/datasets/nyctaxi"
	"github.com/iskorotkov/avro/bench/datasets/superhero"
	gennyctaxi "github.com/iskorotkov/avro/bench/formats/gogenavro/nyctaxi"
	gensuperhero "github.com/iskorotkov/avro/bench/formats/gogenavro/superhero"
)

func fromSuperhero(h *superhero.Superhero) *gensuperhero.Superhero {
	powers := make([]gensuperhero.Superpower, len(h.Powers))
	for i, p := range h.Powers {
		powers[i] = gensuperhero.Superpower{
			Id:      p.ID,
			Name:    p.Name,
			Damage:  p.Damage,
			Energy:  p.Energy,
			Passive: p.Passive,
		}
	}

	return &gensuperhero.Superhero{
		Id:             h.ID,
		Affiliation_id: h.AffiliationID,
		Name:           h.Name,
		Life:           h.Life,
		Energy:         h.Energy,
		Powers:         powers,
	}
}

// fromTrip stores timestamps as epoch microseconds because the library has no logical types.
func fromTrip(t *nyctaxi.Trip) *gennyctaxi.Trip {
	return &gennyctaxi.Trip{
		Vendor_id:             t.VendorID,
		Pickup_time:           t.PickupTime.UnixMicro(),
		Dropoff_time:          t.DropoffTime.UnixMicro(),
		Passenger_count:       nullable(t.PassengerCount, longUnion),
		Trip_distance:         t.TripDistance,
		Ratecode_id:           nullable(t.RatecodeID, longUnion),
		Store_and_fwd_flag:    nullable(t.StoreAndFwdFlag, stringUnion),
		Pickup_location_id:    t.PickupLocationID,
		Dropoff_location_id:   t.DropoffLocationID,
		Payment_type:          t.PaymentType,
		Fare_amount:           t.FareAmount,
		Extra:                 t.Extra,
		Mta_tax:               t.MTATax,
		Tip_amount:            t.TipAmount,
		Tolls_amount:          t.TollsAmount,
		Improvement_surcharge: t.ImprovementSurcharge,
		Total_amount:          t.TotalAmount,
		Congestion_surcharge:  nullable(t.CongestionSurcharge, doubleUnion),
		Airport_fee:           nullable(t.AirportFee, doubleUnion),
	}
}

func nullable[T, U any](p *T, wrap func(T) *U) *U {
	if p == nil {
		return nil
	}

	return wrap(*p)
}

func longUnion(v int64) *gennyctaxi.UnionNullLong {
	return &gennyctaxi.UnionNullLong{Long: v, UnionType: gennyctaxi.UnionNullLongTypeEnumLong}
}

func stringUnion(v string) *gennyctaxi.UnionNullString {
	return &gennyctaxi.UnionNullString{String: v, UnionType: gennyctaxi.UnionNullStringTypeEnumString}
}

func doubleUnion(v float64) *gennyctaxi.UnionNullDouble {
	return &gennyctaxi.UnionNullDouble{Double: v, UnionType: gennyctaxi.UnionNullDoubleTypeEnumDouble}
}
