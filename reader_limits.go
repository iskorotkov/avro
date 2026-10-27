package avro

import (
	"errors"
	"fmt"
	"io"
	"math/bits"
)

const (
	// maxBlockPrealloc caps how many items a block header may allocate before they are decoded.
	maxBlockPrealloc = 4096

	// maxZeroWidthItems caps items per decoded value whose schema occupies no bytes on the wire.
	maxZeroWidthItems = 1 << 20
)

var (
	// ErrMaxDepth is reported when recursive values nest deeper than `Config.MaxDepth`.
	ErrMaxDepth = errors.New("nesting depth exceeds `Config.MaxDepth`")

	// ErrMaxSliceAllocSize is reported when a decoded value holds more array elements than `Config.MaxSliceAllocSize`.
	ErrMaxSliceAllocSize = errors.New("size is greater than `Config.MaxSliceAllocSize`")

	// ErrMaxMapAllocSize is reported when a decoded value holds more map entries than `Config.MaxMapAllocSize`.
	ErrMaxMapAllocSize = errors.New("size is greater than `Config.MaxMapAllocSize`")

	// ErrMaxByteSliceSize is reported when a bytes or string value is longer than `Config.MaxByteSliceSize`.
	ErrMaxByteSliceSize = errors.New("size is greater than `Config.MaxByteSliceSize`")

	// ErrTooManyZeroWidthItems is reported when a decoded value holds too many items without wire representation.
	ErrTooManyZeroWidthItems = errors.New("too many items without wire representation")

	// ErrBlockCountExceedsInput is reported when an array or map block count cannot fit in the remaining input.
	ErrBlockCountExceedsInput = fmt.Errorf("block count exceeds remaining input: %w", io.ErrUnexpectedEOF)
)

// readerLimits tracks the resources consumed by the outermost value currently being decoded.
type readerLimits struct {
	open      int
	depth     int
	sliceElem int
	mapElem   int
	zeroWidth int
}

// lengther is implemented by in-memory readers that know how many bytes are left.
type lengther interface {
	Len() int
}

// beginValue starts a new per-value budget unless another value is already being decoded.
func (r *Reader) beginValue() {
	if r.limits.open == 0 {
		r.limits = readerLimits{}
	}
	r.limits.open++
}

func (r *Reader) endValue() {
	r.limits.open--
}

// enterNested records one more nesting level, failing once `Config.MaxDepth` is exceeded.
func (r *Reader) enterNested() bool {
	if r.limits.depth < r.cfg.getMaxDepth() {
		r.limits.depth++
		return true
	}
	r.reportDepthExceeded()
	return false
}

// errDecodeDepth is reported unwrapped so deep values do not build an error message per level.
var errDecodeDepth = fmt.Errorf("avro: decode: %w", ErrMaxDepth)

func (r *Reader) reportDepthExceeded() {
	if r.Error == nil || errors.Is(r.Error, io.EOF) {
		r.Error = errDecodeDepth
	}
}

func (r *Reader) leaveNested() {
	r.limits.depth--
}

// remainingInput returns the number of unread input bytes, or -1 when it is unknown.
func (r *Reader) remainingInput() int {
	buffered := r.tail - r.head
	switch {
	case r.reader == nil:
		return buffered
	case r.sized != nil:
		return buffered + r.sized.Len()
	default:
		return -1
	}
}

// reportErr records err for operation unless an earlier error other than io.EOF is already recorded.
func (r *Reader) reportErr(operation string, err error) {
	if r.Error != nil && !errors.Is(r.Error, io.EOF) {
		return
	}
	r.Error = fmt.Errorf("avro: %s: %w", operation, err)
}

// reportTruncated records an error wrapping io.ErrUnexpectedEOF for a value that cannot fit in the remaining input.
func (r *Reader) reportTruncated(operation, msg string) {
	r.reportErr(operation, fmt.Errorf("%s: %w", msg, io.ErrUnexpectedEOF))
}

// allowArrayBlock reports whether an array block of n items with the given minimum item size may be decoded.
func (r *Reader) allowArrayBlock(n, minItemSize int) bool {
	const op = "decode array"
	return r.charge(&r.limits.sliceElem, n, r.cfg.getMaxSliceAllocSize(), op, ErrMaxSliceAllocSize) &&
		r.allowBlock(op, n, minItemSize)
}

// allowMapBlock reports whether a map block of n entries with the given minimum entry size may be decoded.
func (r *Reader) allowMapBlock(n, minEntrySize int) bool {
	const op = "decode map"
	return r.charge(&r.limits.mapElem, n, r.cfg.getMaxMapAllocSize(), op, ErrMaxMapAllocSize) &&
		r.allowBlock(op, n, minEntrySize)
}

func (r *Reader) allowBlock(op string, n, minItemSize int) bool {
	if minItemSize == 0 {
		return r.charge(&r.limits.zeroWidth, n, maxZeroWidthItems, op, ErrTooManyZeroWidthItems)
	}

	if rem := r.remainingInput(); rem >= 0 && exceeds(n, minItemSize, rem) {
		r.reportErr(op, ErrBlockCountExceedsInput)
		return false
	}
	return true
}

// exceeds reports whether n items of size bytes need more than rem bytes, without overflowing.
func exceeds(n, size, rem int) bool {
	hi, lo := bits.Mul64(uint64(n), uint64(size))
	return hi != 0 || lo > uint64(rem)
}

func (r *Reader) charge(used *int, n, limit int, op string, err error) bool {
	if n > limit-*used {
		r.reportErr(op, err)
		return false
	}
	*used += n
	return true
}
