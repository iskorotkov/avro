// Package nyctaxi provides NYC TLC yellow taxi trip records, a flat numeric-heavy real-life dataset.
package nyctaxi

import (
	_ "embed"
	"fmt"
	"path/filepath"
	"time"

	"github.com/iskorotkov/avro/bench/dataset"
)

// Name is the dataset name.
const Name = "nyctaxi"

const fullFile = "yellow_tripdata_2024-01.parquet"

var (
	//go:embed schema.avsc
	schema string
	//go:embed sample.jsonl.zst
	sample []byte
)

// Trip is the canonical record.
type Trip struct {
	VendorID             int32     `avro:"vendor_id"             json:"vendor_id"`
	PickupTime           time.Time `avro:"pickup_time"           json:"pickup_time"`
	DropoffTime          time.Time `avro:"dropoff_time"          json:"dropoff_time"`
	PassengerCount       *int64    `avro:"passenger_count"       json:"passenger_count"`
	TripDistance         float64   `avro:"trip_distance"         json:"trip_distance"`
	RatecodeID           *int64    `avro:"ratecode_id"           json:"ratecode_id"`
	StoreAndFwdFlag      *string   `avro:"store_and_fwd_flag"    json:"store_and_fwd_flag"`
	PickupLocationID     int32     `avro:"pickup_location_id"    json:"pickup_location_id"`
	DropoffLocationID    int32     `avro:"dropoff_location_id"   json:"dropoff_location_id"`
	PaymentType          int64     `avro:"payment_type"          json:"payment_type"`
	FareAmount           float64   `avro:"fare_amount"           json:"fare_amount"`
	Extra                float64   `avro:"extra"                 json:"extra"`
	MTATax               float64   `avro:"mta_tax"               json:"mta_tax"`
	TipAmount            float64   `avro:"tip_amount"            json:"tip_amount"`
	TollsAmount          float64   `avro:"tolls_amount"          json:"tolls_amount"`
	ImprovementSurcharge float64   `avro:"improvement_surcharge" json:"improvement_surcharge"`
	TotalAmount          float64   `avro:"total_amount"          json:"total_amount"`
	CongestionSurcharge  *float64  `avro:"congestion_surcharge"  json:"congestion_surcharge"`
	AirportFee           *float64  `avro:"airport_fee"           json:"airport_fee"`
}

// Provider loads the NYC taxi dataset.
type Provider struct{}

// Name implements dataset.Provider.
func (Provider) Name() string { return Name }

// Sources implements dataset.Provider.
func (Provider) Sources() []dataset.Source {
	return []dataset.Source{{
		URL:    "https://d37ci6vzurychx.cloudfront.net/trip-data/" + fullFile,
		SHA256: "c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510",
		File:   fullFile,
	}}
}

// Load implements dataset.Provider.
func (Provider) Load(dir string) (dataset.Dataset, error) {
	records, err := load(dir)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", Name, err)
	}

	return dataset.New(Name, schema, records), nil
}

func load(dir string) ([]Trip, error) {
	if dir == "" {
		return dataset.DecodeJSONL[Trip](sample)
	}

	return ReadParquet(filepath.Join(dir, fullFile), 1)
}
