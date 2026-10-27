package otel_test

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/iskorotkov/avro/bench/datasets/otel"
)

func TestGenerateDeterministic(t *testing.T) {
	a, b := otel.Generate(20), otel.Generate(20)
	for i := range a {
		if !a[i].Equal(b[i]) {
			t.Fatalf("record %d differs between runs", i)
		}
	}
}

func TestGenerateInvariants(t *testing.T) {
	kinds := map[string]int{}
	for i, rs := range otel.Generate(200) {
		if len(rs.Resource.Attributes) == 0 || len(rs.ScopeSpans) == 0 {
			t.Fatalf("record %d has no resource attributes or scopes", i)
		}
		for _, ss := range rs.ScopeSpans {
			if len(ss.Spans) == 0 {
				t.Fatalf("record %d has an empty scope", i)
			}
			for _, s := range ss.Spans {
				kinds[s.Kind]++
				if s.EndTime.Before(s.StartTime) {
					t.Fatalf("record %d span %q ends before it starts", i, s.Name)
				}
				for _, e := range s.Events {
					if e.Time.Before(s.StartTime) || e.Time.After(s.EndTime) {
						t.Fatalf("record %d span %q has an event outside its span", i, s.Name)
					}
				}
			}
		}
	}
	for _, k := range []string{"SPAN_KIND_SERVER", "SPAN_KIND_CLIENT", "SPAN_KIND_PRODUCER", "SPAN_KIND_CONSUMER", "SPAN_KIND_INTERNAL"} {
		if kinds[k] == 0 {
			t.Errorf("no %s spans generated", k)
		}
	}
}

func TestEqual(t *testing.T) {
	a, b := otel.Generate(3)[1], otel.Generate(3)[1]
	if !a.Equal(b) {
		t.Fatal("identical records differ")
	}

	b.ScopeSpans[0].Spans[0].Events = nil
	b.ScopeSpans[0].Spans[0].Links = nil
	a.ScopeSpans[0].Spans[0].Events = []otel.Event{}
	a.ScopeSpans[0].Spans[0].Links = []otel.Link{}
	if !a.Equal(b) {
		t.Fatal("nil and empty slices differ")
	}

	b.ScopeSpans[0].Spans[0].Name += "x"
	if a.Equal(b) {
		t.Fatal("changed span name not detected")
	}
}

func TestIDJSON(t *testing.T) {
	id := otel.SpanID{0xde, 0xad, 0xbe, 0xef, 0, 1, 2, 3}
	data, err := json.Marshal(id)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `"deadbeef00010203"` {
		t.Fatalf("got %s", data)
	}

	var back otel.SpanID
	if err := json.Unmarshal(data, &back); err != nil || back != id {
		t.Fatalf("round trip: %v, %v", back, err)
	}
	if err := json.Unmarshal([]byte(`"dead"`), &back); err == nil {
		t.Fatal("short id accepted")
	}
}

func TestAnyValueBranches(t *testing.T) {
	for _, rs := range otel.Generate(100) {
		for _, v := range rs.Resource.Attributes {
			if n := branches(v); n != 1 {
				t.Fatalf("resource value sets %d branches", n)
			}
		}
	}
}

func branches(v otel.AnyValue) int {
	return len(slices.DeleteFunc([]bool{
		v.StringValue != nil, v.BoolValue != nil, v.IntValue != nil,
		v.DoubleValue != nil, v.BytesValue != nil, v.ArrayValue != nil,
	}, func(set bool) bool { return !set }))
}
