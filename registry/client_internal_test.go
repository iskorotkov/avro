package registry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient_WithHTTPClient(t *testing.T) {
	httpClient := &http.Client{}

	client, _ := NewClient("http://example.com", WithHTTPClient(httpClient))

	assert.Equal(t, client.client, httpClient)
}

func TestNewClient_WithBasicAuth(t *testing.T) {
	creds := credentials{username: "username", password: "password"}

	client, _ := NewClient("http://example.com", WithBasicAuth("username", "password"))

	assert.Equal(t, client.creds, creds)
}

func TestClient_GetSchemaMissExpires(t *testing.T) {
	var requests atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"schema":"\"int\""}`))
	}))
	t.Cleanup(s.Close)
	now := time.Unix(0, 0)
	client, _ := NewClient(s.URL, WithSchemaMissTTL(time.Second))
	client.now = func() time.Time { return now }

	_, err := client.GetSchema(context.Background(), 7)
	require.Error(t, err)
	_, err = client.GetSchema(context.Background(), 7)
	require.Error(t, err)

	now = now.Add(2 * time.Second)
	schema, err := client.GetSchema(context.Background(), 7)

	require.NoError(t, err)
	assert.Equal(t, `"int"`, schema.String())
	assert.Equal(t, int32(2), requests.Load())
}

func TestClient_WithSchemaCacheSize(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":"\"int\""}`))
	}))
	t.Cleanup(s.Close)
	client, _ := NewClient(s.URL, WithSchemaCacheSize(2))

	for id := range 5 {
		_, err := client.GetSchema(context.Background(), id)
		require.NoError(t, err)
	}

	assert.Equal(t, 2, client.cache.Len())
}
