package ghwebhooks

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// primaryDir holds the github.com payloads; it is read before the GitHub Enterprise Server ones.
const primaryDir = "api.github.com"

// timeKeys are the payload keys holding timestamps, as RFC 3339 strings or Unix seconds.
var timeKeys = map[string]bool{
	"created_at": true, "updated_at": true, "pushed_at": true, "closed_at": true, "merged_at": true,
	"published_at": true, "due_on": true, "suspended_at": true, "timestamp": true,
}

type eventExamples struct {
	Name     string            `json:"name"`
	Examples []json.RawMessage `json:"examples"`
}

// ReadPackage reads the index.json files of an @octokit/webhooks-examples tarball, api.github.com first; a non-empty only keeps just that directory.
func ReadPackage(tgz, only string) ([]Event, error) {
	f, err := os.Open(filepath.Clean(tgz))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	indexes, err := readIndexes(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", tgz, err)
	}

	var events []Event
	for _, dir := range orderDirs(indexes) {
		if only != "" && dir != only {
			continue
		}
		parsed, err := parseIndex(indexes[dir])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", dir, err)
		}
		events = append(events, parsed...)
	}

	return events, nil
}

func readIndexes(r io.Reader) (map[string][]byte, error) {
	zr, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer func() { _ = zr.Close() }()

	indexes := map[string][]byte{}
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return indexes, nil
		} else if err != nil {
			return nil, err
		}
		if path.Base(hdr.Name) != "index.json" || hdr.Typeflag != tar.TypeReg {
			continue
		}
		if indexes[path.Base(path.Dir(hdr.Name))], err = io.ReadAll(tr); err != nil {
			return nil, err
		}
	}
}

func orderDirs(indexes map[string][]byte) []string {
	dirs := make([]string, 0, len(indexes))
	for dir := range indexes {
		dirs = append(dirs, dir)
	}
	slices.SortFunc(dirs, func(a, b string) int {
		return strings.Compare(sortKey(a), sortKey(b))
	})

	return dirs
}

func sortKey(dir string) string {
	if dir == primaryDir {
		return ""
	}

	return dir
}

func parseIndex(data []byte) ([]Event, error) {
	var index []eventExamples
	if err := json.Unmarshal(data, &index); err != nil {
		return nil, err
	}

	var events []Event
	for _, ex := range index {
		for i, payload := range ex.Examples {
			e, err := ParsePayload(ex.Name, payload)
			if err != nil {
				return nil, fmt.Errorf("%s example %d: %w", ex.Name, i, err)
			}
			events = append(events, e)
		}
	}

	return events, nil
}

// ParsePayload decodes one webhook payload, normalising its timestamps to UTC milliseconds.
func ParsePayload(name string, payload []byte) (Event, error) {
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()

	var tree any
	if err := dec.Decode(&tree); err != nil {
		return Event{}, err
	}

	normalise(tree)
	data, err := json.Marshal(tree)
	if err != nil {
		return Event{}, err
	}

	e := Event{Event: name}
	if err := json.Unmarshal(data, &e); err != nil {
		return Event{}, err
	}

	return e, nil
}

func normalise(v any) {
	switch v := v.(type) {
	case map[string]any:
		for k, child := range v {
			if t, ok := timestamp(k, child); ok {
				v[k] = t
				continue
			}
			normalise(child)
		}
	case []any:
		for _, child := range v {
			normalise(child)
		}
	}
}

// timestamp converts a timestamp value to an RFC 3339 UTC string with millisecond precision.
func timestamp(key string, v any) (string, bool) {
	if !timeKeys[key] {
		return "", false
	}

	var t time.Time
	switch v := v.(type) {
	case json.Number:
		sec, err := v.Int64()
		if err != nil {
			return "", false
		}
		t = time.Unix(sec, 0)
	case string:
		var err error
		if t, err = time.Parse(time.RFC3339, v); err != nil {
			return "", false
		}
	default:
		return "", false
	}

	return t.UTC().Truncate(time.Millisecond).Format(time.RFC3339Nano), true
}
