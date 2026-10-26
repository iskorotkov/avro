package avro

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"reflect"
	"unsafe"
)

func createDecoderOfFixed(fixed *FixedSchema, typ reflect.Type) ValDecoder {
	switch typ.Kind() {
	case reflect.Array:
		if typ.Elem().Kind() != reflect.Uint8 || typ.Len() != fixed.Size() {
			break
		}
		return &fixedCodec{size: typ.Len()}
	case reflect.Uint64:
		if fixed.Size() != 8 {
			break
		}

		return &fixedUint64Codec{}
	case reflect.Pointer:
		elemType := typ.Elem()

		ls := fixed.Logical()
		typ1 := elemType
		if elemType.Kind() != reflect.Struct || !typ1.ConvertibleTo(ratType) || ls == nil ||
			ls.Type() != Decimal {
			break
		}
		dec := ls.(*DecimalLogicalSchema)
		return &fixedDecimalCodec{prec: dec.Precision(), scale: dec.Scale(), size: fixed.Size()}
	case reflect.Struct:
		ls := fixed.Logical()
		if ls == nil {
			break
		}
		typ1 := typ
		if !typ1.ConvertibleTo(durType) || ls.Type() != Duration {
			break
		}
		return &fixedDurationCodec{}
	}

	return &errorDecoder{
		err: fmt.Errorf("avro: %s is unsupported for Avro %s, size=%d", typ.String(), fixed.Type(), fixed.Size()),
	}
}

func createEncoderOfFixed(fixed *FixedSchema, typ reflect.Type) ValEncoder {
	switch typ.Kind() {
	case reflect.Array:
		if typ.Elem().Kind() != reflect.Uint8 || typ.Len() != fixed.Size() {
			break
		}
		return &fixedCodec{size: typ.Len()}
	case reflect.Uint64:
		if fixed.Size() != 8 {
			break
		}

		return &fixedUint64Codec{}
	case reflect.Pointer:
		elemType := typ.Elem()

		ls := fixed.Logical()
		typ1 := elemType
		if elemType.Kind() != reflect.Struct || !typ1.ConvertibleTo(ratType) || ls == nil ||
			ls.Type() != Decimal {
			break
		}
		dec := ls.(*DecimalLogicalSchema)
		return &fixedDecimalCodec{prec: dec.Precision(), scale: dec.Scale(), size: fixed.Size()}

	case reflect.Struct:
		ls := fixed.Logical()
		if ls == nil {
			break
		}
		typ1 := typ
		if typ1.ConvertibleTo(durType) && ls.Type() == Duration {
			return &fixedDurationCodec{}
		}
	}

	return &errorEncoder{
		err: fmt.Errorf("avro: %s is unsupported for Avro %s, size=%d", typ.String(), fixed.Type(), fixed.Size()),
	}
}

type fixedUint64Codec [8]byte

func (c *fixedUint64Codec) Decode(ptr unsafe.Pointer, r *Reader) {
	buffer := c[:]
	r.Read(buffer)
	*(*uint64)(ptr) = binary.BigEndian.Uint64(buffer)
}

func (c *fixedUint64Codec) Encode(ptr unsafe.Pointer, w *Writer) {
	buffer := c[:]
	binary.BigEndian.PutUint64(buffer, *(*uint64)(ptr))
	_, _ = w.Write(buffer)
}

type fixedCodec struct {
	size int
}

func (c *fixedCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	b := unsafe.Slice((*byte)(ptr), c.size)
	for i := range b {
		b[i] = r.readByte()
	}
}

func (c *fixedCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	_, _ = w.Write(unsafe.Slice((*byte)(ptr), c.size))
}

type fixedDecimalCodec struct {
	prec  int
	scale int
	size  int
}

func (c *fixedDecimalCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	b := make([]byte, c.size)
	r.Read(b)
	*(**big.Rat)(ptr) = ratFromBytes(b, c.scale)
}

func (c *fixedDecimalCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	r := *(**big.Rat)(ptr)
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(c.scale)), nil)
	i := (&big.Int{}).Mul(r.Num(), scale)
	i = i.Div(i, r.Denom())

	if numDigits, ok := checkDecimalPrecision(i, c.prec); !ok {
		w.Error = fmt.Errorf(
			"avro: cannot encode %v as Avro fixed.decimal with precision=%d, has %d significant digits",
			r.FloatString(c.scale),
			c.prec,
			numDigits,
		)
		return
	}

	var b []byte
	switch i.Sign() {
	case 0:
		b = make([]byte, c.size)

	case 1:
		b = i.Bytes()
		if b[0]&0x80 > 0 {
			b = append([]byte{0}, b...)
		}
		if len(b) < c.size {
			padded := make([]byte, c.size)
			copy(padded[c.size-len(b):], b)
			b = padded
		}

	case -1:
		b = i.Add(i, (&big.Int{}).Lsh(one, uint(c.size*8))).Bytes()
	}

	if len(b) != c.size {
		w.Error = fmt.Errorf(
			"avro: cannot encode %v as Avro fixed.decimal with size=%d, encodes to %d bytes",
			r.FloatString(c.scale),
			c.size,
			len(b),
		)
		return
	}

	_, _ = w.Write(b)
}

type fixedDurationCodec struct{}

func (*fixedDurationCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	b := make([]byte, 12)
	r.Read(b)
	var duration LogicalDuration
	duration.Months = binary.LittleEndian.Uint32(b[0:4])
	duration.Days = binary.LittleEndian.Uint32(b[4:8])
	duration.Milliseconds = binary.LittleEndian.Uint32(b[8:12])
	*(*LogicalDuration)(ptr) = duration
}

func (*fixedDurationCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	duration := (*LogicalDuration)(ptr)
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, duration.Months)
	_, _ = w.Write(b)
	binary.LittleEndian.PutUint32(b, duration.Days)
	_, _ = w.Write(b)
	binary.LittleEndian.PutUint32(b, duration.Milliseconds)
	_, _ = w.Write(b)
}
