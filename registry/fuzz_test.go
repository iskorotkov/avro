package registry_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/internal/fuzzcheck"
	"github.com/iskorotkov/avro/v2/registry"
)

// fuzzFrame is a valid Confluent wire-format prefix for schema id 42.
var fuzzFrame = []byte{0x00, 0x00, 0x00, 0x00, 0x2a}

const fuzzSchema = `{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":["null","string"]}]}`

// FuzzDecoderDecode decodes arbitrary payloads framed for a pre-cached registry schema and round-trips valid ones.
func FuzzDecoderDecode(f *testing.F) {
	schema, err := avro.Parse(fuzzSchema)
	if err != nil {
		f.Fatalf("parse schema: %v", err)
	}
	body, err := json.Marshal(map[string]string{"schema": fuzzSchema})
	if err != nil {
		f.Fatalf("marshal schema response: %v", err)
	}
	h := http.NewServeMux()
	h.HandleFunc("/schemas/ids/42", func(rw http.ResponseWriter, _ *http.Request) {
		_, _ = rw.Write(body)
	})
	h.HandleFunc("/", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusNotFound)
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	client, err := registry.NewClient(srv.URL)
	if err != nil {
		f.Fatalf("new client: %v", err)
	}
	if _, err = client.GetSchema(context.Background(), 42); err != nil {
		f.Fatalf("pre-cache schema: %v", err)
	}
	decoder := registry.NewDecoder(client)

	var out any
	for _, bad := range [][]byte{{0x01, 0x00, 0x00, 0x00, 0x2a, 0x00}, {0x00, 0xff, 0xff, 0xff, 0xff, 0x00}} {
		if err = decoder.Decode(context.Background(), bad, &out); err == nil {
			f.Fatalf("decode %x: expected framing error", bad)
		}
	}

	f.Add([]byte{0x02, 0x02, 0x02, 0x61})
	f.Add([]byte{0x02, 0x00})
	f.Add([]byte{})

	decode := func(data []byte) (any, error) {
		var v any
		err := decoder.Decode(context.Background(), append(slices.Clip(fuzzFrame), data...), &v)
		return v, err
	}
	encode := func(v any) ([]byte, error) { return avro.Marshal(schema, v) }

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<16 {
			return
		}
		if _, err := fuzzcheck.Decode(avro.DefaultConfig, schema, data); err != nil {
			_, _ = decode(data)
			return
		}
		fuzzcheck.RoundTrip(t, data, decode, encode)
	})
}
