package avro

import (
	"math/big"
	"sync/atomic"
)

var pow10Cache [DefaultMaxDecimalPrecision + 1]atomic.Pointer[big.Int]

// pow10 returns 10^n, which callers must not modify.
func pow10(n int) *big.Int {
	if n < 0 || n >= len(pow10Cache) {
		return exp10(n)
	}
	if p := pow10Cache[n].Load(); p != nil {
		return p
	}
	p := exp10(n)
	pow10Cache[n].Store(p)
	return p
}

func exp10(n int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

// ratFromBytes decodes a two's-complement unscaled decimal.
func ratFromBytes(b []byte, scale int) *big.Rat {
	num := (&big.Int{}).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 > 0 {
		num.Sub(num, new(big.Int).Lsh(one, uint(len(b))*8))
	}
	return new(big.Rat).SetFrac(num, pow10(scale))
}

// unscaledDecimal returns r multiplied by 10^scale and truncated to an integer.
func unscaledDecimal(r *big.Rat, scale int) *big.Int {
	i := (&big.Int{}).Mul(r.Num(), pow10(scale))
	return i.Div(i, r.Denom())
}

// checkDecimalPrecision checks if the value exceeds the specified precision.
// returns the number of digits and whether it is valid.
func checkDecimalPrecision(value *big.Int, prec int) (int, bool) {
	unscaledAbsStr := new(big.Int).Abs(value).String()
	numDigits := len(unscaledAbsStr)

	if len(unscaledAbsStr) > prec {
		return numDigits, false
	}

	return numDigits, true
}
