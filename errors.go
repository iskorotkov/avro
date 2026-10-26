package avro

import (
	"errors"
	"io"
	"strings"
)

// SemanticError reports an error that occurred while encoding or decoding a record field.
//
// The path covers nested records only: it stops at array, map and union boundaries,
// where the remaining context stays in the message of Err.
// Records decoded into map[string]any and generic decoding do not produce it.
// Input that ends inside or right before a varint is reported without a field path.
type SemanticError struct {
	// Field is the dotted path of Go struct field names, or record field names for map records, e.g. "Address.Zip".
	Field string
	// Err is the underlying error.
	Err error

	path []string
}

// Error keeps the message format of the plain wrapped errors this type replaced.
func (e *SemanticError) Error() string {
	if len(e.path) == 0 {
		return e.Field + ": " + e.Err.Error()
	}
	return strings.Join(e.path, ": ") + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *SemanticError) Unwrap() error { return e.Err }

// wrapFieldError prefixes a record field name to the error path.
func wrapFieldError(name string, err error) error {
	path := []string{name}
	//nolint:errorlint // Only a directly nested field error may be merged without losing context.
	if inner, ok := err.(*SemanticError); ok && len(inner.path) > 0 {
		path = append(path, inner.path...)
		err = inner.Err
	}
	return &SemanticError{Field: strings.Join(path, "."), Err: err, path: path}
}

// ShortBufferError reports that Unmarshal input ended before the value was complete.
// It covers input cut inside a varint, between record fields and empty input for a value that needs bytes.
// Bytes left after a complete value are ignored.
type ShortBufferError struct {
	// Have is the length of the whole input passed to Unmarshal.
	Have int
	// Err is the underlying error, which matches io.ErrUnexpectedEOF.
	Err error
}

// Error returns the message of the underlying error.
func (e *ShortBufferError) Error() string { return e.Err.Error() }

// Unwrap returns the underlying error.
func (e *ShortBufferError) Unwrap() error { return e.Err }

// wrapShortBuffer marks truncated-input errors as ShortBufferError.
func wrapShortBuffer(err error, have int) error {
	if err == nil || !errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}
	return &ShortBufferError{Have: have, Err: err}
}
