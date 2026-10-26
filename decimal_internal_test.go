package avro

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPow10(t *testing.T) {
	assert.Equal(t, big.NewInt(1000), pow10(3))
	assert.Same(t, pow10(DefaultMaxDecimalPrecision), pow10(DefaultMaxDecimalPrecision))
	assert.Equal(t, 0, exp10(DefaultMaxDecimalPrecision+1).Cmp(pow10(DefaultMaxDecimalPrecision+1)))
}
