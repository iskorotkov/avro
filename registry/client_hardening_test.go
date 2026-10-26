package registry_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/iskorotkov/avro/v2/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const hostileSubject = "../../tenant-b/subjects/secret?permanent=true#x"

func TestClient_EscapesSubjectOnEveryEndpoint(t *testing.T) {
	const escaped = "..%2F..%2Ftenant-b%2Fsubjects%2Fsecret%3Fpermanent=true%23x"
	ctx := context.Background()

	tests := []struct {
		name   string
		method string
		path   string
		call   func(c *registry.Client) error
	}{
		{"DeleteSubject", http.MethodDelete, "subjects/" + escaped, func(c *registry.Client) error {
			_, err := c.DeleteSubject(ctx, hostileSubject)
			return err
		}},
		{"GetVersions", http.MethodGet, "subjects/" + escaped + "/versions", func(c *registry.Client) error {
			_, err := c.GetVersions(ctx, hostileSubject)
			return err
		}},
		{"GetSchemaByVersion", http.MethodGet, "subjects/" + escaped + "/versions/2", func(c *registry.Client) error {
			_, err := c.GetSchemaByVersion(ctx, hostileSubject, 2)
			return err
		}},
		{"GetLatestSchema", http.MethodGet, "subjects/" + escaped + "/versions/latest", func(c *registry.Client) error {
			_, err := c.GetLatestSchema(ctx, hostileSubject)
			return err
		}},
		{"GetSchemaInfo", http.MethodGet, "subjects/" + escaped + "/versions/2", func(c *registry.Client) error {
			_, err := c.GetSchemaInfo(ctx, hostileSubject, 2)
			return err
		}},
		{"GetLatestSchemaInfo", http.MethodGet, "subjects/" + escaped + "/versions/latest", func(c *registry.Client) error {
			_, err := c.GetLatestSchemaInfo(ctx, hostileSubject)
			return err
		}},
		{"CreateSchema", http.MethodPost, "subjects/" + escaped + "/versions", func(c *registry.Client) error {
			_, _, err := c.CreateSchema(ctx, hostileSubject, `"int"`)
			return err
		}},
		{"IsRegistered", http.MethodPost, "subjects/" + escaped, func(c *registry.Client) error {
			_, _, err := c.IsRegistered(ctx, hostileSubject, `"int"`)
			return err
		}},
		{"IsCompatible", http.MethodPost, "compatibility/subjects/" + escaped + "/versions", func(c *registry.Client) error {
			_, err := c.IsCompatible(ctx, hostileSubject, `"int"`)
			return err
		}},
		{"SetCompatibilityLevel", http.MethodPut, "config/" + escaped, func(c *registry.Client) error {
			return c.SetCompatibilityLevel(ctx, hostileSubject, registry.FullCL)
		}},
		{"GetCompatibilityLevel", http.MethodGet, "config/" + escaped, func(c *registry.Client) error {
			_, err := c.GetCompatibilityLevel(ctx, hostileSubject)
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var gotMethod, gotURI string
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod, gotURI = r.Method, r.RequestURI
				_, _ = w.Write([]byte(`{}`))
			}))
			t.Cleanup(s.Close)
			client, err := registry.NewClient(s.URL + "/tenant-a/")
			require.NoError(t, err)

			_ = test.call(client)

			assert.Equal(t, test.method, gotMethod)
			assert.Equal(t, "/tenant-a/"+test.path, gotURI)
		})
	}
}

func TestClient_RejectsInvalidSubjectWithoutRequest(t *testing.T) {
	var requests atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	for _, subject := range []string{"", ".", "..", "a\nb"} {
		_, err := client.DeleteSubject(context.Background(), subject)

		assert.ErrorIs(t, err, registry.ErrInvalidPathSegment, "subject %q", subject)
	}
	assert.Zero(t, requests.Load())
}

func TestClient_LimitsResponseSize(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`["` + strings.Repeat("a", 1024) + `"]`))
	}))
	t.Cleanup(s.Close)

	limited, _ := registry.NewClient(s.URL, registry.WithMaxResponseBytes(512))
	_, err := limited.GetSubjects(context.Background())
	var maxErr *http.MaxBytesError
	assert.True(t, errors.As(err, &maxErr), "got %v", err)
	assert.ErrorContains(t, err, "response body exceeds 512 bytes")

	unlimited, _ := registry.NewClient(s.URL, registry.WithMaxResponseBytes(0))
	subjects, err := unlimited.GetSubjects(context.Background())
	require.NoError(t, err)
	assert.Len(t, subjects, 1)
}

func TestClient_GetSchemaCachesNotFound(t *testing.T) {
	var requests atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error_code":40403,"message":"Schema not found"}`))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, err1 := client.GetSchema(context.Background(), 7)
	_, err2 := client.GetSchema(context.Background(), 7)

	require.Error(t, err1)
	assert.Equal(t, err1, err2)
	assert.Equal(t, int32(1), requests.Load())
}

func TestClient_GetSchemaDoesNotCacheServerErrors(t *testing.T) {
	var requests atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)

	_, _ = client.GetSchema(context.Background(), 7)
	_, _ = client.GetSchema(context.Background(), 7)

	assert.Equal(t, int32(2), requests.Load())
}

func TestClient_GetSchemaMissCacheDisabled(t *testing.T) {
	var requests atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL, registry.WithSchemaMissTTL(0))

	_, _ = client.GetSchema(context.Background(), 7)
	_, _ = client.GetSchema(context.Background(), 7)

	assert.Equal(t, int32(2), requests.Load())
}

func TestClient_RegisteringSchemaClearsMiss(t *testing.T) {
	tests := []struct {
		name     string
		register func(c *registry.Client) error
	}{
		{"CreateSchema", func(c *registry.Client) error {
			_, _, err := c.CreateSchema(context.Background(), "foo", `"int"`)
			return err
		}},
		{"IsRegistered", func(c *registry.Client) error {
			_, _, err := c.IsRegistered(context.Background(), "foo", `"int"`)
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var registered atomic.Bool
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost:
					registered.Store(true)
					_, _ = w.Write([]byte(`{"id":7}`))
				case registered.Load():
					_, _ = w.Write([]byte(`{"schema":"\"int\""}`))
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			t.Cleanup(s.Close)
			client, _ := registry.NewClient(s.URL)

			_, err := client.GetSchema(context.Background(), 7)
			require.Error(t, err)
			require.NoError(t, test.register(client))
			schema, err := client.GetSchema(context.Background(), 7)

			require.NoError(t, err)
			assert.Equal(t, `"int"`, schema.String())
		})
	}
}

func respondWith(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.Close)
	return s
}

// padded returns s with its padding string grown so the result is exactly size bytes.
func padded(t *testing.T, size int, prefix, suffix string) string {
	t.Helper()
	n := size - len(prefix) - len(suffix)
	require.GreaterOrEqual(t, n, 0)
	return prefix + strings.Repeat("a", n) + suffix
}

func TestClient_MaxResponseBytesBoundary(t *testing.T) {
	const limit = 64
	ctx := context.Background()
	tests := []struct {
		name string
		body func(t *testing.T, size int) string
		call func(c *registry.Client) error
	}{
		{
			"GetSubjects",
			func(t *testing.T, size int) string { return padded(t, size, `["`, `"]`) },
			func(c *registry.Client) error { _, err := c.GetSubjects(ctx); return err },
		},
		{
			"GetSchema",
			func(t *testing.T, size int) string { return padded(t, size, `{"schema":"\"int\"","x":"`, `"}`) },
			func(c *registry.Client) error { _, err := c.GetSchema(ctx, 1); return err },
		},
	}

	for _, test := range tests {
		for _, tc := range []struct {
			name string
			size int
			ok   bool
		}{
			{"limit-1", limit - 1, true},
			{"limit", limit, true},
			{"limit+1", limit + 1, false},
		} {
			t.Run(test.name+"/"+tc.name, func(t *testing.T) {
				s := respondWith(t, http.StatusOK, test.body(t, tc.size))
				client, err := registry.NewClient(s.URL, registry.WithMaxResponseBytes(limit))
				require.NoError(t, err)

				err = test.call(client)

				if tc.ok {
					require.NoError(t, err)
					return
				}
				var maxErr *http.MaxBytesError
				require.ErrorAs(t, err, &maxErr)
				assert.Equal(t, int64(limit), maxErr.Limit)
				assert.ErrorContains(t, err, "response body exceeds 64 bytes")
			})
		}
	}
}

func TestClient_OversizedSchemaResponseIsNotRemembered(t *testing.T) {
	var requests atomic.Int32
	body := padded(t, 65, `{"schema":"\"int\"","x":"`, `"}`)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL, registry.WithMaxResponseBytes(64))

	_, err1 := client.GetSchema(context.Background(), 1)
	_, err2 := client.GetSchema(context.Background(), 1)

	require.Error(t, err1)
	require.Error(t, err2)
	assert.Equal(t, int32(2), requests.Load())
}

func TestClient_MaxResponseBytesAppliesToErrorBodies(t *testing.T) {
	const limit = 64
	const prefix = `{"error_code":40401,"message":"`
	const suffix = `"}`
	tests := []struct {
		name        string
		size        int
		wantMessage string
	}{
		{"limit", limit, strings.Repeat("a", limit-len(prefix)-len(suffix))},
		{"limit+1", limit + 1, ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := respondWith(t, http.StatusNotFound, padded(t, test.size, prefix, suffix))
			client, _ := registry.NewClient(s.URL, registry.WithMaxResponseBytes(limit))

			_, err := client.GetSubjects(context.Background())

			regErr, ok := errors.AsType[registry.Error](err)
			require.True(t, ok, "got %v", err)
			assert.Equal(t, http.StatusNotFound, regErr.StatusCode)
			assert.Equal(t, test.wantMessage, regErr.Message)
		})
	}
}

func TestClient_RequestFailsWithoutContext(t *testing.T) {
	var requests atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
	}))
	t.Cleanup(s.Close)
	client, _ := registry.NewClient(s.URL)
	var ctx context.Context

	_, err := client.GetSubjects(ctx)

	assert.ErrorContains(t, err, "nil Context")
	assert.Zero(t, requests.Load())
}
