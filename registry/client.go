// Package registry implements a Confluent Schema Registry compliant client.
//
// See the Confluent Schema Registry docs for an understanding of the
// API: https://docs.confluent.io/current/schema-registry/docs/api.html
package registry

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/registry/internal/boundedmap"
)

const contentType = "application/vnd.schemaregistry.v1+json"

const (
	// DefaultMaxResponseBytes is the default cap on a registry response body.
	DefaultMaxResponseBytes = 8 << 20

	// DefaultSchemaMissTTL is how long a schema ID the registry did not return is remembered by default.
	DefaultSchemaMissTTL = 5 * time.Second

	maxSchemaMisses = 1024
)

// Registry represents a schema registry.
type Registry interface {
	// GetSchema returns the schema with the given id.
	GetSchema(ctx context.Context, id int) (avro.Schema, error)

	// DeleteSubject delete subject.
	DeleteSubject(ctx context.Context, subject string) ([]int, error)

	// GetSubjects gets the registry subjects.
	GetSubjects(ctx context.Context) ([]string, error)

	// GetVersions gets the schema versions for a subject.
	GetVersions(ctx context.Context, subject string) ([]int, error)

	// GetSchemaByVersion gets the schema by version.
	GetSchemaByVersion(ctx context.Context, subject string, version int) (avro.Schema, error)

	// GetLatestSchema gets the latest schema for a subject.
	GetLatestSchema(ctx context.Context, subject string) (avro.Schema, error)

	// GetSchemaInfo gets the schema and schema metadata for a subject and version.
	GetSchemaInfo(ctx context.Context, subject string, version int) (SchemaInfo, error)

	// GetLatestSchemaInfo gets the latest schema and schema metadata for a subject.
	GetLatestSchemaInfo(ctx context.Context, subject string) (SchemaInfo, error)

	// CreateSchema creates a schema in the registry, returning the schema id.
	CreateSchema(ctx context.Context, subject, schema string, references ...SchemaReference) (int, avro.Schema, error)

	// IsRegistered determines if the schema is registered.
	IsRegistered(ctx context.Context, subject, schema string) (int, avro.Schema, error)

	// IsRegisteredWithRefs determines if the schema is registered, with optional referenced schemas.
	IsRegisteredWithRefs(ctx context.Context, subject, schema string, refs ...SchemaReference) (int, avro.Schema, error)

	// IsCompatible determines if the schema is compatible with all schemas in the subject.
	IsCompatible(ctx context.Context, subject, schema string) (bool, error)

	// IsCompatibleWithRefs determines if the schema is compatible with all schemas in the subject,
	// with optional referenced schemas.
	IsCompatibleWithRefs(ctx context.Context, subject, schema string, refs ...SchemaReference) (bool, error)
}

type schemaPayload struct {
	Schema     string            `json:"schema"`
	References []SchemaReference `json:"references,omitempty"`
}

// SchemaReference represents a schema reference.
type SchemaReference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

type idPayload struct {
	ID int `json:"id"`
}

type credentials struct {
	username string
	password string
}

type schemaInfoPayload struct {
	Schema   string         `json:"schema"`
	ID       int            `json:"id"`
	Version  int            `json:"version"`
	Metadata schemaMetadata `json:"metadata"`
}

type schemaMetadata struct {
	Properties map[string]string `json:"properties"`
}

// Parse converts the string schema registry response into a
// SchemaInfo object with an avro.Schema schema.
func (s *schemaInfoPayload) Parse() (info SchemaInfo, err error) {
	info = SchemaInfo{
		ID:      s.ID,
		Version: s.Version,
		Metadata: SchemaMetadata{
			Properties: s.Metadata.Properties,
		},
	}
	info.Schema, err = avro.Parse(s.Schema)
	return info, err
}

// SchemaInfo represents a schema and metadata information.
type SchemaInfo struct {
	Schema   avro.Schema
	ID       int
	Version  int
	Metadata SchemaMetadata
}

// IDBytes returns 4 bytes that represent the schema ID.
func (s SchemaInfo) IDBytes() []byte {
	result := make([]byte, 4)
	binary.BigEndian.PutUint32(result, uint32(s.ID))
	return result
}

// SchemaMetadata represents the schema metadata.
type SchemaMetadata struct {
	Properties map[string]string
}

var defaultClient = &http.Client{
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 90 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 3 * time.Second,
		IdleConnTimeout:     90 * time.Second,
	},
	Timeout: 10 * time.Second,
}

// ClientFunc is a function used to customize the Client.
type ClientFunc func(*Client)

// WithHTTPClient sets the http client to make requests with.
func WithHTTPClient(client *http.Client) ClientFunc {
	return func(c *Client) {
		c.client = client
	}
}

// WithBasicAuth sets the credentials to perform http basic auth.
func WithBasicAuth(username, password string) ClientFunc {
	return func(c *Client) {
		c.creds = credentials{username: username, password: password}
	}
}

// WithMaxResponseBytes caps the size of a registry response body; n <= 0 disables the cap.
func WithMaxResponseBytes(n int64) ClientFunc {
	return func(c *Client) {
		c.maxResponseBytes = n
	}
}

// WithSchemaCacheSize bounds the number of schemas cached by ID; n <= 0 means unbounded.
func WithSchemaCacheSize(n int) ClientFunc {
	return func(c *Client) {
		c.cacheSize = n
	}
}

// WithSchemaMissTTL sets how long a failed schema ID lookup is remembered; d <= 0 disables it.
func WithSchemaMissTTL(d time.Duration) ClientFunc {
	return func(c *Client) {
		c.missTTL = d
	}
}

// Client is an HTTP registry client.
type Client struct {
	client *http.Client
	base   *url.URL

	creds            credentials
	maxResponseBytes int64
	cacheSize        int
	missTTL          time.Duration
	now              func() time.Time

	cache  *boundedmap.Map[int, avro.Schema]
	misses *boundedmap.Map[int, schemaMiss]
}

type schemaMiss struct {
	err     error
	expires time.Time
}

// NewClient creates a schema registry Client with the given base url.
func NewClient(baseURL string, opts ...ClientFunc) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}

	c := &Client{
		client:           defaultClient,
		base:             u,
		maxResponseBytes: DefaultMaxResponseBytes,
		missTTL:          DefaultSchemaMissTTL,
		now:              time.Now,
	}

	for _, opt := range opts {
		opt(c)
	}

	c.cache = boundedmap.New[int, avro.Schema](c.cacheSize)
	c.misses = boundedmap.New[int, schemaMiss](maxSchemaMisses)

	return c, nil
}

// GetSchema returns the schema with the given id.
//
// GetSchema will cache the schema in memory after it is successfully returned,
// allowing it to be used efficiently in a high load situation.
// IDs the registry reports as not found, and schemas that fail to parse,
// are remembered for the miss TTL so repeated lookups do not hit the registry.
func (c *Client) GetSchema(ctx context.Context, id int) (avro.Schema, error) {
	if schema, ok := c.cache.Load(id); ok {
		return schema, nil
	}
	if err := c.cachedMiss(id); err != nil {
		return nil, err
	}

	var resp schemaPayload
	p := []string{"schemas", "ids", strconv.Itoa(id)}
	if err := c.request(ctx, http.MethodGet, p, nil, &resp); err != nil {
		if regErr, ok := errors.AsType[Error](err); ok && regErr.StatusCode == http.StatusNotFound {
			c.storeMiss(id, err)
		}
		return nil, err
	}

	schema, err := avro.Parse(resp.Schema)
	if err != nil {
		c.storeMiss(id, err)
		return nil, err
	}

	c.cache.Store(id, schema)

	return schema, nil
}

func (c *Client) cachedMiss(id int) error {
	miss, ok := c.misses.Load(id)
	if !ok {
		return nil
	}
	if c.now().After(miss.expires) {
		c.misses.Delete(id)
		return nil
	}
	return miss.err
}

func (c *Client) storeMiss(id int, err error) {
	if c.missTTL <= 0 {
		return
	}
	c.misses.Store(id, schemaMiss{err: err, expires: c.now().Add(c.missTTL)})
}

// GetSubjects gets the registry subjects.
func (c *Client) GetSubjects(ctx context.Context) ([]string, error) {
	var subjects []string
	if err := c.request(ctx, http.MethodGet, []string{"subjects"}, nil, &subjects); err != nil {
		return nil, err
	}
	return subjects, nil
}

// DeleteSubject delete subject.
func (c *Client) DeleteSubject(ctx context.Context, subject string) ([]int, error) {
	var versions []int
	p := []string{"subjects", subject}
	if err := c.request(ctx, http.MethodDelete, p, nil, &versions); err != nil {
		return nil, err
	}

	return versions, nil
}

// GetVersions gets the schema versions for a subject.
func (c *Client) GetVersions(ctx context.Context, subject string) ([]int, error) {
	var versions []int
	p := []string{"subjects", subject, "versions"}
	if err := c.request(ctx, http.MethodGet, p, nil, &versions); err != nil {
		return nil, err
	}
	return versions, nil
}

// GetSchemaByVersion gets the schema by version.
func (c *Client) GetSchemaByVersion(ctx context.Context, subject string, version int) (avro.Schema, error) {
	var resp schemaPayload
	p := []string{"subjects", subject, "versions", strconv.Itoa(version)}
	if err := c.request(ctx, http.MethodGet, p, nil, &resp); err != nil {
		return nil, err
	}
	return avro.Parse(resp.Schema)
}

// GetLatestSchema gets the latest schema for a subject.
func (c *Client) GetLatestSchema(ctx context.Context, subject string) (avro.Schema, error) {
	var resp schemaPayload
	p := []string{"subjects", subject, "versions", "latest"}
	if err := c.request(ctx, http.MethodGet, p, nil, &resp); err != nil {
		return nil, err
	}
	return avro.Parse(resp.Schema)
}

// GetSchemaInfo gets the schema and schema metadata for a subject and version.
func (c *Client) GetSchemaInfo(ctx context.Context, subject string, version int) (SchemaInfo, error) {
	var resp schemaInfoPayload
	p := []string{"subjects", subject, "versions", strconv.Itoa(version)}
	if err := c.request(ctx, http.MethodGet, p, nil, &resp); err != nil {
		return SchemaInfo{}, err
	}
	return resp.Parse()
}

// GetLatestSchemaInfo gets the latest schema and schema metadata for a subject.
func (c *Client) GetLatestSchemaInfo(ctx context.Context, subject string) (SchemaInfo, error) {
	var resp schemaInfoPayload
	p := []string{"subjects", subject, "versions", "latest"}
	if err := c.request(ctx, http.MethodGet, p, nil, &resp); err != nil {
		return SchemaInfo{}, err
	}
	return resp.Parse()
}

// CreateSchema creates a schema in the registry, returning the schema id.
func (c *Client) CreateSchema(
	ctx context.Context,
	subject, schema string,
	references ...SchemaReference,
) (int, avro.Schema, error) {
	var resp idPayload
	req := schemaPayload{Schema: schema, References: references}
	p := []string{"subjects", subject, "versions"}
	if err := c.request(ctx, http.MethodPost, p, req, &resp); err != nil {
		return 0, nil, err
	}

	sch, err := avro.Parse(schema)
	return resp.ID, sch, err
}

// IsRegistered determines if the schema is registered.
func (c *Client) IsRegistered(ctx context.Context, subject, schema string) (int, avro.Schema, error) {
	return c.IsRegisteredWithRefs(ctx, subject, schema)
}

// IsRegisteredWithRefs determines if the schema is registered, with optional referenced schemas.
func (c *Client) IsRegisteredWithRefs(
	ctx context.Context,
	subject, schema string,
	references ...SchemaReference,
) (int, avro.Schema, error) {
	var resp idPayload
	req := schemaPayload{Schema: schema, References: references}
	if err := c.request(ctx, http.MethodPost, []string{"subjects", subject}, req, &resp); err != nil {
		return 0, nil, err
	}

	sch, err := avro.Parse(schema)
	return resp.ID, sch, err
}

type isCompatibleResponse struct {
	IsCompatible bool `json:"is_compatible"`
}

// IsCompatible determines if the schema is compatible with all schemas in the subject.
func (c *Client) IsCompatible(ctx context.Context, subject, schema string) (bool, error) {
	return c.IsCompatibleWithRefs(ctx, subject, schema)
}

// IsCompatibleWithRefs determines if the schema is compatible with all schemas in the subject,
// with optional referenced schemas.
func (c *Client) IsCompatibleWithRefs(
	ctx context.Context,
	subject,
	schema string,
	references ...SchemaReference,
) (bool, error) {
	req := schemaPayload{Schema: schema, References: references}
	reqPath := []string{"compatibility", "subjects", subject, "versions"}
	var resp isCompatibleResponse
	if err := c.request(ctx, http.MethodPost, reqPath, req, &resp); err != nil {
		return false, err
	}
	return resp.IsCompatible, nil
}

// Compatibility levels.
const (
	BackwardCL           string = "BACKWARD"
	BackwardTransitiveCL string = "BACKWARD_TRANSITIVE"
	ForwardCL            string = "FORWARD"
	ForwardTransitiveCL  string = "FORWARD_TRANSITIVE"
	FullCL               string = "FULL"
	FullTransitiveCL     string = "FULL_TRANSITIVE"
	NoneCL               string = "NONE"
)

func validateCompatibilityLevel(lvl string) error {
	switch lvl {
	case BackwardCL, BackwardTransitiveCL, ForwardCL, ForwardTransitiveCL, FullCL, FullTransitiveCL, NoneCL:
		return nil
	default:
		return fmt.Errorf("invalid compatibility level %s", lvl)
	}
}

type compatPayload struct {
	Compatibility string `json:"compatibility"`
}

// SetGlobalCompatibilityLevel sets the global compatibility level of the registry.
func (c *Client) SetGlobalCompatibilityLevel(ctx context.Context, lvl string) error {
	if err := validateCompatibilityLevel(lvl); err != nil {
		return err
	}

	req := compatPayload{Compatibility: lvl}
	return c.request(ctx, http.MethodPut, []string{"config"}, req, nil)
}

// SetCompatibilityLevel sets the compatibility level of a subject.
func (c *Client) SetCompatibilityLevel(ctx context.Context, subject, lvl string) error {
	if err := validateCompatibilityLevel(lvl); err != nil {
		return err
	}

	req := compatPayload{Compatibility: lvl}
	return c.request(ctx, http.MethodPut, []string{"config", subject}, req, nil)
}

// GetGlobalCompatibilityLevel gets the global compatibility level.
func (c *Client) GetGlobalCompatibilityLevel(ctx context.Context) (string, error) {
	var resp compatPayload
	if err := c.request(ctx, http.MethodGet, []string{"config"}, nil, &resp); err != nil {
		return "", err
	}
	return resp.Compatibility, nil
}

// GetCompatibilityLevel gets the compatibility level of a subject.
func (c *Client) GetCompatibilityLevel(ctx context.Context, subject string) (string, error) {
	var resp compatPayload
	if err := c.request(ctx, http.MethodGet, []string{"config", subject}, nil, &resp); err != nil {
		return "", err
	}
	return resp.Compatibility, nil
}

func (c *Client) request(ctx context.Context, method string, segments []string, in, out any) error {
	u, err := endpoint(c.base, segments...)
	if err != nil {
		return err
	}

	var body io.Reader
	if in != nil {
		b, _ := json.Marshal(in)
		body = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)

	if len(c.creds.username) > 0 || len(c.creds.password) > 0 {
		req.SetBasicAuth(c.creds.username, c.creds.password)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("could not perform request: %w", err)
	}
	respBody := c.limitBody(resp.Body)
	defer func() {
		_, _ = io.Copy(io.Discard, respBody)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= http.StatusBadRequest {
		err := Error{StatusCode: resp.StatusCode}
		_ = json.NewDecoder(respBody).Decode(&err)
		return err
	}

	if out != nil {
		return json.NewDecoder(respBody).Decode(out)
	}
	return nil
}

func (c *Client) limitBody(body io.ReadCloser) io.ReadCloser {
	if c.maxResponseBytes <= 0 {
		return body
	}
	return http.MaxBytesReader(nil, body, c.maxResponseBytes)
}

// Error is returned by the registry when there is an error.
type Error struct {
	StatusCode int    `json:"-"`
	Code       int    `json:"error_code"`
	Message    string `json:"message"`
}

// Error returns the error message.
func (e Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "registry error: " + strconv.Itoa(e.StatusCode)
}
