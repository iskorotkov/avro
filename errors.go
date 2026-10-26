package avro

import (
	"errors"
	"io"
	"strings"
)

// SemanticError reports an error that occurred while encoding or decoding a record field.
type SemanticError struct {
	// Field is the dotted path of Go struct field names or record field names, e.g. "Address.Zip".
	Field string
	// Err is the underlying error.
	Err error
}

// Error keeps the message format of the plain wrapped errors this type replaced.
func (e *SemanticError) Error() string {
	return strings.ReplaceAll(e.Field, ".", ": ") + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *SemanticError) Unwrap() error { return e.Err }

// wrapFieldError prefixes a record field name to the error path.
func wrapFieldError(name string, err error) error {
	//nolint:errorlint // Only a directly nested field error may be merged without losing context.
	if inner, ok := err.(*SemanticError); ok {
		return &SemanticError{Field: name + "." + inner.Field, Err: inner.Err}
	}
	return &SemanticError{Field: name, Err: err}
}

// ShortBufferError reports that the input ended before a complete value was decoded.
type ShortBufferError struct {
	// Have is the number of input bytes available.
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
