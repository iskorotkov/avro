package msgp

//go:generate go tool msgp -file types.go -o types_gen.go -tests=false

import "time"

//msgp:tuple off

// Superhero mirrors superhero.Superhero for code generation.
type Superhero struct {
	ID            int32         `msg:"id"`
	AffiliationID int32         `msg:"affiliation_id"`
	Name          string        `msg:"name"`
	Life          float32       `msg:"life"`
	Energy        float32       `msg:"energy"`
	Powers        []*Superpower `msg:"powers"`
}

// Superpower mirrors superhero.Superpower for code generation.
type Superpower struct {
	ID      int32   `msg:"id"`
	Name    string  `msg:"name"`
	Damage  float32 `msg:"damage"`
	Energy  float32 `msg:"energy"`
	Passive bool    `msg:"passive"`
}

// Trip mirrors nyctaxi.Trip for code generation.
type Trip struct {
	VendorID             int32     `msg:"vendor_id"`
	PickupTime           time.Time `msg:"pickup_time"`
	DropoffTime          time.Time `msg:"dropoff_time"`
	PassengerCount       *int64    `msg:"passenger_count"`
	TripDistance         float64   `msg:"trip_distance"`
	RatecodeID           *int64    `msg:"ratecode_id"`
	StoreAndFwdFlag      *string   `msg:"store_and_fwd_flag"`
	PickupLocationID     int32     `msg:"pickup_location_id"`
	DropoffLocationID    int32     `msg:"dropoff_location_id"`
	PaymentType          int64     `msg:"payment_type"`
	FareAmount           float64   `msg:"fare_amount"`
	Extra                float64   `msg:"extra"`
	MTATax               float64   `msg:"mta_tax"`
	TipAmount            float64   `msg:"tip_amount"`
	TollsAmount          float64   `msg:"tolls_amount"`
	ImprovementSurcharge float64   `msg:"improvement_surcharge"`
	TotalAmount          float64   `msg:"total_amount"`
	CongestionSurcharge  *float64  `msg:"congestion_surcharge"`
	AirportFee           *float64  `msg:"airport_fee"`
}
