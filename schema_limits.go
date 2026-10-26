package avro

import "sync/atomic"

const (
	// DefaultMaxFixedSize is the default largest fixed size the parser accepts.
	DefaultMaxFixedSize = 1 << 20
	// DefaultMaxDecimalPrecision is the default largest decimal precision the parser accepts.
	DefaultMaxDecimalPrecision = 1000
)

var (
	maxFixedSize        = newParseLimit(DefaultMaxFixedSize)
	maxDecimalPrecision = newParseLimit(DefaultMaxDecimalPrecision)
)

// MaxFixedSize returns the largest fixed size the parser accepts; values <= 0 mean no limit.
func MaxFixedSize() int {
	return maxFixedSize.get()
}

// SetMaxFixedSize sets the largest fixed size the parser accepts, or none if n <= 0, and returns the previous limit.
func SetMaxFixedSize(n int) int {
	return maxFixedSize.set(n)
}

// MaxDecimalPrecision returns the largest decimal precision the parser accepts; values <= 0 mean no limit.
func MaxDecimalPrecision() int {
	return maxDecimalPrecision.get()
}

// SetMaxDecimalPrecision sets the largest decimal precision the parser accepts, or none if n <= 0, and returns the previous limit.
func SetMaxDecimalPrecision(n int) int {
	return maxDecimalPrecision.set(n)
}

// parseLimit is a parser limit that can be changed while schemas are being parsed.
type parseLimit struct {
	n atomic.Int64
}

func newParseLimit(n int) *parseLimit {
	l := &parseLimit{}
	l.n.Store(int64(n))
	return l
}

func (l *parseLimit) get() int {
	return int(l.n.Load())
}

func (l *parseLimit) set(n int) int {
	return int(l.n.Swap(int64(n)))
}

// exceeds reports whether v is above a positive limit, returning the limit it checked.
func (l *parseLimit) exceeds(v int64) (int64, bool) {
	limit := l.n.Load()
	return limit, limit > 0 && v > limit
}
