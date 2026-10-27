package otel

import (
	"bytes"
	"maps"
	"slices"
)

// Equal reports whether two records match, treating nil and empty slices and maps alike.
func (r ResourceSpans) Equal(o ResourceSpans) bool {
	return r.SchemaURL == o.SchemaURL &&
		r.Resource.DroppedAttributesCount == o.Resource.DroppedAttributesCount &&
		attrsEqual(r.Resource.Attributes, o.Resource.Attributes) &&
		slices.EqualFunc(r.ScopeSpans, o.ScopeSpans, scopeEqual)
}

func scopeEqual(a, b ScopeSpans) bool {
	return a.SchemaURL == b.SchemaURL &&
		a.Scope.Name == b.Scope.Name &&
		a.Scope.Version == b.Scope.Version &&
		a.Scope.DroppedAttributesCount == b.Scope.DroppedAttributesCount &&
		attrsEqual(a.Scope.Attributes, b.Scope.Attributes) &&
		slices.EqualFunc(a.Spans, b.Spans, spanEqual)
}

func spanEqual(a, b Span) bool {
	return a.TraceID == b.TraceID &&
		a.SpanID == b.SpanID &&
		ptrEqual(a.ParentSpanID, b.ParentSpanID) &&
		a.TraceState == b.TraceState &&
		a.Name == b.Name &&
		a.Kind == b.Kind &&
		a.StartTime.Equal(b.StartTime) &&
		a.EndTime.Equal(b.EndTime) &&
		a.DroppedAttributesCount == b.DroppedAttributesCount &&
		a.DroppedEventsCount == b.DroppedEventsCount &&
		a.DroppedLinksCount == b.DroppedLinksCount &&
		a.Status == b.Status &&
		attrsEqual(a.Attributes, b.Attributes) &&
		slices.EqualFunc(a.Events, b.Events, eventEqual) &&
		slices.EqualFunc(a.Links, b.Links, linkEqual)
}

func eventEqual(a, b Event) bool {
	return a.Time.Equal(b.Time) &&
		a.Name == b.Name &&
		a.DroppedAttributesCount == b.DroppedAttributesCount &&
		attrsEqual(a.Attributes, b.Attributes)
}

func linkEqual(a, b Link) bool {
	return a.TraceID == b.TraceID &&
		a.SpanID == b.SpanID &&
		a.TraceState == b.TraceState &&
		a.DroppedAttributesCount == b.DroppedAttributesCount &&
		attrsEqual(a.Attributes, b.Attributes)
}

func attrsEqual(a, b map[string]AnyValue) bool { return maps.EqualFunc(a, b, valueEqual) }

func valueEqual(a, b AnyValue) bool {
	return ptrEqual(a.StringValue, b.StringValue) &&
		ptrEqual(a.BoolValue, b.BoolValue) &&
		ptrEqual(a.IntValue, b.IntValue) &&
		ptrEqual(a.DoubleValue, b.DoubleValue) &&
		(a.BytesValue == nil) == (b.BytesValue == nil) &&
		bytes.Equal(a.BytesValue, b.BytesValue) &&
		(a.ArrayValue == nil) == (b.ArrayValue == nil) &&
		slices.EqualFunc(a.ArrayValue, b.ArrayValue, valueEqual)
}

func ptrEqual[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}

	return *a == *b
}
