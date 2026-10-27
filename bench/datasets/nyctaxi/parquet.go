package nyctaxi

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/parquet-go/parquet-go"
)

// row mirrors the TLC parquet columns.
type row struct {
	VendorID             *int32   `parquet:"VendorID"`
	PickupTime           *int64   `parquet:"tpep_pickup_datetime"`
	DropoffTime          *int64   `parquet:"tpep_dropoff_datetime"`
	PassengerCount       *int64   `parquet:"passenger_count"`
	TripDistance         *float64 `parquet:"trip_distance"`
	RatecodeID           *int64   `parquet:"RatecodeID"`
	StoreAndFwdFlag      *string  `parquet:"store_and_fwd_flag"`
	PickupLocationID     *int32   `parquet:"PULocationID"`
	DropoffLocationID    *int32   `parquet:"DOLocationID"`
	PaymentType          *int64   `parquet:"payment_type"`
	FareAmount           *float64 `parquet:"fare_amount"`
	Extra                *float64 `parquet:"extra"`
	MTATax               *float64 `parquet:"mta_tax"`
	TipAmount            *float64 `parquet:"tip_amount"`
	TollsAmount          *float64 `parquet:"tolls_amount"`
	ImprovementSurcharge *float64 `parquet:"improvement_surcharge"`
	TotalAmount          *float64 `parquet:"total_amount"`
	CongestionSurcharge  *float64 `parquet:"congestion_surcharge"`
	AirportFee           *float64 `parquet:"Airport_fee"`
}

// ReadParquet reads every stride-th trip from a TLC yellow taxi parquet file, stopping after rows trips when rows is positive.
func ReadParquet(path string, stride, rows int) ([]Trip, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	r := parquet.NewGenericReader[row](f)
	defer func() { _ = r.Close() }()

	limit := int(r.NumRows())/stride + 1
	if rows > 0 {
		limit = min(limit, rows)
	}

	trips := make([]Trip, 0, limit)
	buf := make([]row, 1024)
	for index := 0; len(trips) < limit; {
		n, err := r.Read(buf)
		for i := range buf[:n] {
			if (index+i)%stride == 0 && len(trips) < limit {
				trips = append(trips, buf[i].trip())
			}
		}
		index += n

		if errors.Is(err, io.EOF) {
			return trips, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
	}

	return trips, nil
}

func (r *row) trip() Trip {
	return Trip{
		VendorID:             value(r.VendorID),
		PickupTime:           micros(r.PickupTime),
		DropoffTime:          micros(r.DropoffTime),
		PassengerCount:       r.PassengerCount,
		TripDistance:         value(r.TripDistance),
		RatecodeID:           r.RatecodeID,
		StoreAndFwdFlag:      r.StoreAndFwdFlag,
		PickupLocationID:     value(r.PickupLocationID),
		DropoffLocationID:    value(r.DropoffLocationID),
		PaymentType:          value(r.PaymentType),
		FareAmount:           value(r.FareAmount),
		Extra:                value(r.Extra),
		MTATax:               value(r.MTATax),
		TipAmount:            value(r.TipAmount),
		TollsAmount:          value(r.TollsAmount),
		ImprovementSurcharge: value(r.ImprovementSurcharge),
		TotalAmount:          value(r.TotalAmount),
		CongestionSurcharge:  r.CongestionSurcharge,
		AirportFee:           r.AirportFee,
	}
}

func value[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}

	return *p
}

// micros converts a TLC timestamp, a wall-clock time in New York stored as UTC micros.
func micros(p *int64) time.Time {
	return time.UnixMicro(value(p)).UTC()
}
