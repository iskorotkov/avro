package avro_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_TextUnmarshalerPtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x28, 0x32, 0x30, 0x32, 0x30, 0x2d, 0x30, 0x31, 0x2d, 0x30, 0x32, 0x54, 0x30, 0x33, 0x3a, 0x30, 0x34, 0x3a, 0x30, 0x35, 0x5a}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var ts TestTimestampPtr
	err = dec.Decode(&ts)

	require.NoError(t, err)
	want := TestTimestampPtr(time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC))
	assert.Equal(t, want, ts)
}

func TestDecoder_TextUnmarshalerPtrPtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x28, 0x32, 0x30, 0x32, 0x30, 0x2d, 0x30, 0x31, 0x2d, 0x30, 0x32, 0x54, 0x30, 0x33, 0x3a, 0x30, 0x34, 0x3a, 0x30, 0x35, 0x5a}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var ts *TestTimestampPtr
	err = dec.Decode(&ts)

	require.NoError(t, err)
	assert.NotNil(t, ts)
	want := TestTimestampPtr(time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC))
	assert.Equal(t, want, *ts)
}

func TestDecoder_TextUnmarshalerError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x28, 0x32, 0x30, 0x32, 0x30, 0x2d, 0x30, 0x31, 0x2d, 0x30, 0x32, 0x54, 0x30, 0x33, 0x3a, 0x30, 0x34, 0x3a, 0x30, 0x35, 0x5a}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var ts *TestTimestampError
	err = dec.Decode(&ts)

	assert.Error(t, err)
}

func TestEncoder_TextMarshaler(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)
	ts := TestTimestamp(time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC))

	err = enc.Encode(ts)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x28, 0x32, 0x30, 0x32, 0x30, 0x2d, 0x30, 0x31, 0x2d, 0x30, 0x32, 0x54, 0x30, 0x33, 0x3a, 0x30, 0x34, 0x3a, 0x30, 0x35, 0x5a}, buf.Bytes())
}

func TestEncoder_TextMarshalerPtr(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)
	ts := TestTimestampPtr(time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC))

	err = enc.Encode(&ts)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x28, 0x32, 0x30, 0x32, 0x30, 0x2d, 0x30, 0x31, 0x2d, 0x30, 0x32, 0x54, 0x30, 0x33, 0x3a, 0x30, 0x34, 0x3a, 0x30, 0x35, 0x5a}, buf.Bytes())
}

func TestEncoder_TextMarshalerPtrNil(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)
	var ts *TestTimestampPtr

	err = enc.Encode(ts)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00}, buf.Bytes())
}

func TestEncoder_TextMarshalerError(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)
	ts := TestTimestampError{}

	err = enc.Encode(&ts)

	assert.Error(t, err)
}

type TestTimestamp time.Time

func (t TestTimestamp) MarshalText() ([]byte, error) {
	return (time.Time)(t).MarshalText()
}

type TestTimestampPtr time.Time

func (t *TestTimestampPtr) UnmarshalText(data []byte) error {
	return (*time.Time)(t).UnmarshalText(data)
}

func (t *TestTimestampPtr) MarshalText() ([]byte, error) {
	return (*time.Time)(t).MarshalText()
}

type TestTimestampError time.Time

func (t *TestTimestampError) UnmarshalText(data []byte) error {
	return errors.New("test")
}

func (t *TestTimestampError) MarshalText() ([]byte, error) {
	return nil, errors.New("test")
}

type TestTimestampAppender time.Time

func (t TestTimestampAppender) AppendText(b []byte) ([]byte, error) {
	return (time.Time)(t).AppendText(b)
}

func (t TestTimestampAppender) MarshalText() ([]byte, error) {
	return (time.Time)(t).MarshalText()
}

type TestTimestampAppenderPtr time.Time

func (t *TestTimestampAppenderPtr) AppendText(b []byte) ([]byte, error) {
	return (*time.Time)(t).AppendText(b)
}

func (t *TestTimestampAppenderPtr) MarshalText() ([]byte, error) {
	return (*time.Time)(t).MarshalText()
}

type TestTimestampAppenderError struct{}

func (t TestTimestampAppenderError) AppendText(b []byte) ([]byte, error) {
	return nil, errors.New("test")
}

func (t TestTimestampAppenderError) MarshalText() ([]byte, error) {
	return nil, errors.New("test")
}

func TestEncoder_TextAppender(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)
	ts := TestTimestampAppender(time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC))

	err = enc.Encode(ts)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x28, 0x32, 0x30, 0x32, 0x30, 0x2d, 0x30, 0x31, 0x2d, 0x30, 0x32, 0x54, 0x30, 0x33, 0x3a, 0x30, 0x34, 0x3a, 0x30, 0x35, 0x5a}, buf.Bytes())
}

func TestEncoder_TextAppenderPtr(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)
	ts := TestTimestampAppenderPtr(time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC))

	err = enc.Encode(&ts)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x28, 0x32, 0x30, 0x32, 0x30, 0x2d, 0x30, 0x31, 0x2d, 0x30, 0x32, 0x54, 0x30, 0x33, 0x3a, 0x30, 0x34, 0x3a, 0x30, 0x35, 0x5a}, buf.Bytes())
}

func TestEncoder_TextAppenderPtrNil(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)
	var ts *TestTimestampAppenderPtr

	err = enc.Encode(ts)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00}, buf.Bytes())
}

func TestEncoder_TextAppenderError(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(TestTimestampAppenderError{})

	assert.Error(t, err)
}
