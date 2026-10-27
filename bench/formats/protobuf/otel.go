package protobuf

import (
	"github.com/iskorotkov/avro/bench/datasets/otel"
)

func fromResourceSpans(r *otel.ResourceSpans) *OtelResourceSpans {
	scopes := make([]*OtelScopeSpans, len(r.ScopeSpans))
	for i := range r.ScopeSpans {
		scopes[i] = otelScopeSpans(&r.ScopeSpans[i])
	}

	return &OtelResourceSpans{
		Resource: &OtelResource{
			Attributes:             otelAttrs(r.Resource.Attributes),
			DroppedAttributesCount: r.Resource.DroppedAttributesCount,
		},
		SchemaUrl:  r.SchemaURL,
		ScopeSpans: scopes,
	}
}

func otelScopeSpans(s *otel.ScopeSpans) *OtelScopeSpans {
	spans := make([]*OtelSpan, len(s.Spans))
	for i := range s.Spans {
		spans[i] = otelSpan(&s.Spans[i])
	}

	return &OtelScopeSpans{
		Scope: &OtelInstrumentationScope{
			Name:                   s.Scope.Name,
			Version:                s.Scope.Version,
			Attributes:             otelAttrs(s.Scope.Attributes),
			DroppedAttributesCount: s.Scope.DroppedAttributesCount,
		},
		SchemaUrl: s.SchemaURL,
		Spans:     spans,
	}
}

func otelSpan(s *otel.Span) *OtelSpan {
	events := make([]*OtelEvent, len(s.Events))
	for i, e := range s.Events {
		events[i] = &OtelEvent{
			TimeUnixMicro:          e.Time.UnixMicro(),
			Name:                   e.Name,
			Attributes:             otelAttrs(e.Attributes),
			DroppedAttributesCount: e.DroppedAttributesCount,
		}
	}
	links := make([]*OtelLink, len(s.Links))
	for i, l := range s.Links {
		links[i] = &OtelLink{
			TraceId:                l.TraceID[:],
			SpanId:                 l.SpanID[:],
			TraceState:             l.TraceState,
			Attributes:             otelAttrs(l.Attributes),
			DroppedAttributesCount: l.DroppedAttributesCount,
		}
	}

	out := &OtelSpan{
		TraceId:                s.TraceID[:],
		SpanId:                 s.SpanID[:],
		TraceState:             s.TraceState,
		Name:                   s.Name,
		Kind:                   enum[OtelSpanKind](OtelSpanKind_value, "OTEL_"+s.Kind),
		StartTimeUnixMicro:     s.StartTime.UnixMicro(),
		EndTimeUnixMicro:       s.EndTime.UnixMicro(),
		Attributes:             otelAttrs(s.Attributes),
		DroppedAttributesCount: s.DroppedAttributesCount,
		Events:                 events,
		DroppedEventsCount:     s.DroppedEventsCount,
		Links:                  links,
		DroppedLinksCount:      s.DroppedLinksCount,
		Status: &OtelStatus{
			Code:    enum[OtelStatusCode](OtelStatusCode_value, "OTEL_"+s.Status.Code),
			Message: s.Status.Message,
		},
	}
	if s.ParentSpanID != nil {
		out.ParentSpanId = s.ParentSpanID[:]
	}

	return out
}

func otelAttrs(in map[string]otel.AnyValue) map[string]*OtelAnyValue {
	out := make(map[string]*OtelAnyValue, len(in))
	for k, v := range in {
		out[k] = otelAnyValue(v)
	}

	return out
}

func otelAnyValue(v otel.AnyValue) *OtelAnyValue {
	switch {
	case v.StringValue != nil:
		return &OtelAnyValue{Value: &OtelAnyValue_StringValue{StringValue: *v.StringValue}}
	case v.BoolValue != nil:
		return &OtelAnyValue{Value: &OtelAnyValue_BoolValue{BoolValue: *v.BoolValue}}
	case v.IntValue != nil:
		return &OtelAnyValue{Value: &OtelAnyValue_IntValue{IntValue: *v.IntValue}}
	case v.DoubleValue != nil:
		return &OtelAnyValue{Value: &OtelAnyValue_DoubleValue{DoubleValue: *v.DoubleValue}}
	case v.BytesValue != nil:
		return &OtelAnyValue{Value: &OtelAnyValue_BytesValue{BytesValue: v.BytesValue}}
	case v.ArrayValue != nil:
		values := make([]*OtelAnyValue, len(v.ArrayValue))
		for i, e := range v.ArrayValue {
			values[i] = otelAnyValue(e)
		}

		return &OtelAnyValue{Value: &OtelAnyValue_ArrayValue{ArrayValue: &OtelArrayValue{Values: values}}}
	default:
		return &OtelAnyValue{}
	}
}
