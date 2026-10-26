package avro_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaxFixedSize_DefaultsToOneMiB(t *testing.T) {
	assert.Equal(t, 1<<20, avro.DefaultMaxFixedSize)
	assert.Equal(t, avro.DefaultMaxFixedSize, avro.MaxFixedSize())
}

func TestMaxFixedSize_Boundary(t *testing.T) {
	tests := []struct {
		name    string
		limit   int
		size    int
		wantErr string
	}{
		{name: "Default Limit", limit: avro.DefaultMaxFixedSize, size: avro.DefaultMaxFixedSize},
		{name: "Default Limit Exceeded", limit: avro.DefaultMaxFixedSize, size: avro.DefaultMaxFixedSize + 1, wantErr: "avro: fixed size 1048577 exceeds the maximum of 1048576"},
		{name: "Below Limit", limit: 16, size: 15},
		{name: "At Limit", limit: 16, size: 16},
		{name: "Limit Exceeded", limit: 16, size: 17, wantErr: "avro: fixed size 17 exceeds the maximum of 16"},
		{name: "Zero Disables", limit: 0, size: avro.DefaultMaxFixedSize + 1},
		{name: "Negative Disables", limit: -1, size: avro.DefaultMaxFixedSize + 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer avro.SetMaxFixedSize(avro.SetMaxFixedSize(test.limit))

			schema, err := avro.ParseWithCache(`{"type":"fixed","name":"F","size":`+strconv.Itoa(test.size)+`}`, "", &avro.SchemaCache{})

			if test.wantErr != "" {
				require.EqualError(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.size, schema.(*avro.FixedSchema).Size())
		})
	}
}

func TestSetMaxFixedSize_ReturnsPreviousLimit(t *testing.T) {
	defer avro.SetMaxFixedSize(avro.MaxFixedSize())

	assert.Equal(t, avro.DefaultMaxFixedSize, avro.SetMaxFixedSize(8))
	assert.Equal(t, 8, avro.SetMaxFixedSize(9))
	assert.Equal(t, 9, avro.MaxFixedSize())
}

func TestMaxDecimalPrecision_DefaultsToOneThousand(t *testing.T) {
	assert.Equal(t, 1000, avro.DefaultMaxDecimalPrecision)
	assert.Equal(t, avro.DefaultMaxDecimalPrecision, avro.MaxDecimalPrecision())
}

func TestMaxDecimalPrecision_Boundary(t *testing.T) {
	tests := []struct {
		name        string
		limit       int
		precision   int
		wantDecimal bool
	}{
		{name: "Default Limit", limit: avro.DefaultMaxDecimalPrecision, precision: avro.DefaultMaxDecimalPrecision, wantDecimal: true},
		{name: "Default Limit Exceeded", limit: avro.DefaultMaxDecimalPrecision, precision: avro.DefaultMaxDecimalPrecision + 1},
		{name: "Below Limit", limit: 10, precision: 9, wantDecimal: true},
		{name: "At Limit", limit: 10, precision: 10, wantDecimal: true},
		{name: "Limit Exceeded", limit: 10, precision: 11},
		{name: "Zero Disables", limit: 0, precision: avro.DefaultMaxDecimalPrecision + 1, wantDecimal: true},
		{name: "Negative Disables", limit: -1, precision: avro.DefaultMaxDecimalPrecision + 1, wantDecimal: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer avro.SetMaxDecimalPrecision(avro.SetMaxDecimalPrecision(test.limit))

			schema, err := avro.ParseWithCache(`{"type":"bytes","logicalType":"decimal","precision":`+strconv.Itoa(test.precision)+`}`, "", &avro.SchemaCache{})

			require.NoError(t, err)
			logical := schema.(*avro.PrimitiveSchema).Logical()
			if !test.wantDecimal {
				assert.Nil(t, logical)
				return
			}
			require.NotNil(t, logical)
			assert.Equal(t, test.precision, logical.(*avro.DecimalLogicalSchema).Precision())
		})
	}
}

func TestSetMaxDecimalPrecision_ReturnsPreviousLimit(t *testing.T) {
	defer avro.SetMaxDecimalPrecision(avro.MaxDecimalPrecision())

	assert.Equal(t, avro.DefaultMaxDecimalPrecision, avro.SetMaxDecimalPrecision(5))
	assert.Equal(t, 5, avro.SetMaxDecimalPrecision(6))
	assert.Equal(t, 6, avro.MaxDecimalPrecision())
}

func TestParseLimits_ConcurrentSetAndParse(t *testing.T) {
	defer avro.SetMaxFixedSize(avro.MaxFixedSize())
	defer avro.SetMaxDecimalPrecision(avro.MaxDecimalPrecision())

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for j := range 100 {
				avro.SetMaxFixedSize(16 + (i+j)%16)
				avro.SetMaxDecimalPrecision(16 + (i+j)%16)
			}
		})
		wg.Go(func() {
			for range 100 {
				schema, err := avro.ParseWithCache(`{"type":"fixed","name":"F","size":8,"logicalType":"decimal","precision":16}`, "", &avro.SchemaCache{})
				assert.NoError(t, err)
				assert.NotNil(t, schema.(*avro.FixedSchema).Logical())
			}
		})
	}
	wg.Wait()
}

func TestMaxFixedSize_DisabledRejectsSizeOverflowingInt(t *testing.T) {
	defer avro.SetMaxFixedSize(avro.SetMaxFixedSize(0))

	schema, err := avro.ParseWithCache(`{"type":"fixed","name":"F","size":1099511627776}`, "", &avro.SchemaCache{})

	if strconv.IntSize == 32 {
		require.EqualError(t, err, "avro: fixed size 1099511627776 overflows int")
		return
	}
	require.NoError(t, err)
	assert.Equal(t, int64(1)<<40, int64(schema.(*avro.FixedSchema).Size()))
}
