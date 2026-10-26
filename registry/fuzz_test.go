package registry_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/iskorotkov/avro/v2/registry"
)

// FuzzDecoderDecode decodes arbitrary Confluent wire-format payloads against a registry serving schema id 42.
func FuzzDecoderDecode(f *testing.F) {
	h := http.NewServeMux()
	h.HandleFunc("/schemas/ids/42", func(rw http.ResponseWriter, _ *http.Request) {
		_, _ = rw.Write([]byte(`{"schema":"{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":[\"null\",\"string\"]}]}"}`))
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
	decoder := registry.NewDecoder(client)

	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x2a, 0x02, 0x02, 0x02, 0x61})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x2a, 0x02, 0x00})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x2a})
	f.Add([]byte{0x01, 0x00, 0x00, 0x00, 0x2a, 0x00})
	f.Add([]byte{0x00, 0xff, 0xff, 0xff, 0xff, 0x00})

	f.Fuzz(func(_ *testing.T, data []byte) {
		if len(data) > 1<<16 {
			return
		}
		var out any
		_ = decoder.Decode(context.Background(), data, &out)
	})
}
