package dataset

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// ReadJSONL decodes zstd-compressed JSON Lines into records of type T.
func ReadJSONL[T any](r io.Reader) ([]T, error) {
	zr, err := zstd.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("open zstd stream: %w", err)
	}
	defer zr.Close()

	var records []T
	dec := json.NewDecoder(bufio.NewReader(zr))
	for {
		var v T
		if err := dec.Decode(&v); errors.Is(err, io.EOF) {
			return records, nil
		} else if err != nil {
			return nil, fmt.Errorf("decode record %d: %w", len(records), err)
		}
		records = append(records, v)
	}
}

// DecodeJSONL decodes an in-memory zstd-compressed JSON Lines sample.
func DecodeJSONL[T any](data []byte) ([]T, error) {
	return ReadJSONL[T](bytes.NewReader(data))
}

// WriteJSONL writes records as JSON Lines compressed at the best zstd level.
func WriteJSONL[T any](w io.Writer, records []T) error {
	zw, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		return fmt.Errorf("open zstd stream: %w", err)
	}

	enc := json.NewEncoder(zw)
	for i := range records {
		if err := enc.Encode(&records[i]); err != nil {
			_ = zw.Close()
			return fmt.Errorf("encode record %d: %w", i, err)
		}
	}

	return zw.Close()
}
