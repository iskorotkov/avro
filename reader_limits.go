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

	// unknownWireSize disables the input-size and zero-width checks for callers that cannot size items.
	unknownWireSize = -1
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

func (r *Reader) reportDepthExceeded() {
	r.ReportError("decode", "nesting depth exceeds `Config.MaxDepth`")
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

// reportTruncated records an error wrapping io.ErrUnexpectedEOF for a value that cannot fit in the remaining input.
func (r *Reader) reportTruncated(operation, msg string) {
	if r.Error != nil && !errors.Is(r.Error, io.EOF) {
		return
	}
	r.Error = fmt.Errorf("avro: %s: %s: %w", operation, msg, io.ErrUnexpectedEOF)
}

// allowArrayBlock reports whether an array block of n items with the given minimum item size may be decoded.
func (r *Reader) allowArrayBlock(n, minItemSize int) bool {
	const op = "decode array"
	return r.charge(&r.limits.sliceElem, n, r.cfg.getMaxSliceAllocSize(), op, "size is greater than `Config.MaxSliceAllocSize`") &&
		r.allowBlock(op, n, minItemSize)
}

// allowMapBlock reports whether a map block of n entries with the given minimum entry size may be decoded.
func (r *Reader) allowMapBlock(n, minEntrySize int) bool {
	const op = "decode map"
	return r.charge(&r.limits.mapElem, n, r.cfg.getMaxMapAllocSize(), op, "size is greater than `Config.MaxMapAllocSize`") &&
		r.allowBlock(op, n, minEntrySize)
}

func (r *Reader) allowBlock(op string, n, minItemSize int) bool {
	switch {
	case minItemSize < 0:
		return true
	case minItemSize == 0:
		return r.charge(&r.limits.zeroWidth, n, maxZeroWidthItems, op, "too many items without wire representation")
	}

	if rem := r.remainingInput(); rem >= 0 && exceeds(n, minItemSize, rem) {
		r.ReportError(op, "block count exceeds remaining input")
		return false
	}
	return true
}

// exceeds reports whether n items of size bytes need more than rem bytes, without overflowing.
func exceeds(n, size, rem int) bool {
	hi, lo := bits.Mul64(uint64(n), uint64(size))
	return hi != 0 || lo > uint64(rem)
}

func (r *Reader) charge(used *int, n, limit int, op, msg string) bool {
	if n > limit-*used {
		r.ReportError(op, msg)
		return false
	}
	*used += n
	return true
}
