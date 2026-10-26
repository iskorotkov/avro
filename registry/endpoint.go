package registry

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode"
)

// ErrInvalidPathSegment is returned when a subject cannot be used as a URL path segment.
var ErrInvalidPathSegment = errors.New("registry: invalid path segment")

// endpoint resolves segments against base, escaping each one as a single path segment.
func endpoint(base *url.URL, segments ...string) (*url.URL, error) {
	escaped := make([]string, len(segments))
	for i, s := range segments {
		if err := validateSegment(s); err != nil {
			return nil, err
		}
		escaped[i] = url.PathEscape(s)
	}

	u := *base
	u.Path = base.Path + strings.Join(segments, "/")
	u.RawPath = base.EscapedPath() + strings.Join(escaped, "/")
	u.RawQuery = ""
	u.Fragment = ""
	u.RawFragment = ""
	return &u, nil
}

func validateSegment(s string) error {
	if s == "" || s == "." || s == ".." {
		return fmt.Errorf("%w: %q", ErrInvalidPathSegment, s)
	}
	if strings.ContainsFunc(s, unicode.IsControl) {
		return fmt.Errorf("%w: %q contains control characters", ErrInvalidPathSegment, s)
	}
	return nil
}
