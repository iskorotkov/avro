// Package otel provides a synthetic OpenTelemetry trace dataset: map-heavy spans with events, links and nullable-branch attribute values.
package otel

import (
	_ "embed"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/iskorotkov/avro/bench/dataset"
)

// Name is the dataset name.
const Name = "otel"

var (
	//go:embed schema.avsc
	schema string
	//go:embed sample.jsonl.zst
	sample []byte
)

// TraceID identifies a trace; it is hex in JSON.
type TraceID [16]byte

// SpanID identifies a span; it is hex in JSON.
type SpanID [8]byte

// MarshalText implements encoding.TextMarshaler.
func (id TraceID) MarshalText() ([]byte, error) { return hex.AppendEncode(nil, id[:]), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (id *TraceID) UnmarshalText(text []byte) error { return decodeHex(id[:], text) }

// MarshalText implements encoding.TextMarshaler.
func (id SpanID) MarshalText() ([]byte, error) { return hex.AppendEncode(nil, id[:]), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (id *SpanID) UnmarshalText(text []byte) error { return decodeHex(id[:], text) }

func decodeHex(dst, text []byte) error {
	if hex.DecodedLen(len(text)) != len(dst) {
		return fmt.Errorf("id has %d hex digits, want %d", len(text), 2*len(dst))
	}
	_, err := hex.Decode(dst, text)

	return err
}

// ResourceSpans is the canonical record: one exported batch of spans from a single resource.
type ResourceSpans struct {
	Resource   Resource     `avro:"resource"    json:"resource"`
	SchemaURL  string       `avro:"schema_url"  json:"schema_url"`
	ScopeSpans []ScopeSpans `avro:"scope_spans" json:"scope_spans"`
}

// Resource describes the entity producing the telemetry.
type Resource struct {
	Attributes             map[string]AnyValue `avro:"attributes"               json:"attributes"`
	DroppedAttributesCount int32               `avro:"dropped_attributes_count" json:"dropped_attributes_count"`
}

// AnyValue is a record of nullable branches, like the OTLP JSON AnyValue; exactly one is set.
type AnyValue struct {
	StringValue *string    `avro:"string_value" json:"string_value"`
	BoolValue   *bool      `avro:"bool_value"   json:"bool_value"`
	IntValue    *int64     `avro:"int_value"    json:"int_value"`
	DoubleValue *float64   `avro:"double_value" json:"double_value"`
	BytesValue  []byte     `avro:"bytes_value"  json:"bytes_value"`
	ArrayValue  []AnyValue `avro:"array_value"  json:"array_value"`
}

// ScopeSpans groups the spans of one instrumentation scope.
type ScopeSpans struct {
	Scope     InstrumentationScope `avro:"scope"      json:"scope"`
	SchemaURL string               `avro:"schema_url" json:"schema_url"`
	Spans     []Span               `avro:"spans"      json:"spans"`
}

// InstrumentationScope identifies the instrumentation library.
type InstrumentationScope struct {
	Name                   string              `avro:"name"                     json:"name"`
	Version                string              `avro:"version"                  json:"version"`
	Attributes             map[string]AnyValue `avro:"attributes"               json:"attributes"`
	DroppedAttributesCount int32               `avro:"dropped_attributes_count" json:"dropped_attributes_count"`
}

// Span is a unit of work in a trace.
type Span struct {
	TraceID                TraceID             `avro:"trace_id"                 json:"trace_id"`
	SpanID                 SpanID              `avro:"span_id"                  json:"span_id"`
	ParentSpanID           *SpanID             `avro:"parent_span_id"           json:"parent_span_id"`
	TraceState             string              `avro:"trace_state"              json:"trace_state"`
	Name                   string              `avro:"name"                     json:"name"`
	Kind                   string              `avro:"kind"                     json:"kind"`
	StartTime              time.Time           `avro:"start_time"               json:"start_time"`
	EndTime                time.Time           `avro:"end_time"                 json:"end_time"`
	Attributes             map[string]AnyValue `avro:"attributes"               json:"attributes"`
	DroppedAttributesCount int32               `avro:"dropped_attributes_count" json:"dropped_attributes_count"`
	Events                 []Event             `avro:"events"                   json:"events"`
	DroppedEventsCount     int32               `avro:"dropped_events_count"     json:"dropped_events_count"`
	Links                  []Link              `avro:"links"                    json:"links"`
	DroppedLinksCount      int32               `avro:"dropped_links_count"      json:"dropped_links_count"`
	Status                 Status              `avro:"status"                   json:"status"`
}

// Event is a timestamped, log-like annotation of a span.
type Event struct {
	Time                   time.Time           `avro:"time"                     json:"time"`
	Name                   string              `avro:"name"                     json:"name"`
	Attributes             map[string]AnyValue `avro:"attributes"               json:"attributes"`
	DroppedAttributesCount int32               `avro:"dropped_attributes_count" json:"dropped_attributes_count"`
}

// Link refers to a span in the same or another trace.
type Link struct {
	TraceID                TraceID             `avro:"trace_id"                 json:"trace_id"`
	SpanID                 SpanID              `avro:"span_id"                  json:"span_id"`
	TraceState             string              `avro:"trace_state"              json:"trace_state"`
	Attributes             map[string]AnyValue `avro:"attributes"               json:"attributes"`
	DroppedAttributesCount int32               `avro:"dropped_attributes_count" json:"dropped_attributes_count"`
}

// Status is the outcome of a span.
type Status struct {
	Code    string `avro:"code"    json:"code"`
	Message string `avro:"message" json:"message"`
}

// Provider loads the OpenTelemetry trace dataset.
type Provider struct{}

// Name implements dataset.Provider.
func (Provider) Name() string { return Name }

// Sources implements dataset.Provider; the dataset is synthetic, so only the sample exists.
func (Provider) Sources() []dataset.Source { return nil }

// Load implements dataset.Provider.
func (Provider) Load(_ string, rows int) (dataset.Dataset, error) {
	records, err := dataset.DecodeJSONL[ResourceSpans](sample)
	if err != nil {
		return nil, fmt.Errorf("%s sample: %w", Name, err)
	}

	return dataset.New(Name, schema, dataset.Head(records, rows)), nil
}
