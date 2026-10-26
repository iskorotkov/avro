package registry

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		base     string
		segments []string
		want     string
	}{
		{name: "plain", base: "http://h/", segments: []string{"subjects", "foo"}, want: "http://h/subjects/foo"},
		{name: "base path kept", base: "http://h/tenant-a/", segments: []string{"config"}, want: "http://h/tenant-a/config"},
		{name: "base query dropped", base: "http://h/a/?x=1#f", segments: []string{"subjects"}, want: "http://h/a/subjects"},
		{name: "slash escaped", base: "http://h/", segments: []string{"subjects", "a/b"}, want: "http://h/subjects/a%2Fb"},
		{name: "query escaped", base: "http://h/", segments: []string{"subjects", "a?permanent=true"}, want: "http://h/subjects/a%3Fpermanent=true"},
		{name: "fragment escaped", base: "http://h/", segments: []string{"subjects", "a#b"}, want: "http://h/subjects/a%23b"},
		{name: "dots inside escaped", base: "http://h/t/", segments: []string{"subjects", "../../x"}, want: "http://h/t/subjects/..%2F..%2Fx"},
		{name: "escaped base kept", base: "http://h/a%2Fb/", segments: []string{"config"}, want: "http://h/a%2Fb/config"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base, err := url.Parse(test.base)
			require.NoError(t, err)

			got, err := endpoint(base, test.segments...)

			require.NoError(t, err)
			assert.Equal(t, test.want, got.String())
		})
	}
}

func TestEndpoint_RejectsInvalidSegments(t *testing.T) {
	base, _ := url.Parse("http://h/")

	for _, seg := range []string{"", ".", "..", "a\nb", "a\x00b", "a\rb", "a\x7fb"} {
		_, err := endpoint(base, "subjects", seg)

		assert.ErrorIs(t, err, ErrInvalidPathSegment, "segment %q", seg)
	}
}
