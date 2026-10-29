package buildversion

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFromBuildInfo(t *testing.T) {
	const modulePath = "github.com/iskorotkov/avro/v2"

	tests := []struct {
		name string
		info *debug.BuildInfo
		want string
	}{
		{
			name: "tagged module",
			info: &debug.BuildInfo{Main: debug.Module{Path: modulePath, Version: "v2.36.0"}},
			want: "v2.36.0",
		},
		{
			name: "replaced module",
			info: &debug.BuildInfo{Main: debug.Module{
				Path: modulePath, Version: "v2.36.0",
				Replace: &debug.Module{Path: "../avro", Version: "(devel)"},
			}},
			want: "(devel)",
		},
		{
			name: "missing version",
			info: &debug.BuildInfo{Main: debug.Module{Path: modulePath}},
			want: "unknown",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, fromBuildInfo(test.info))
		})
	}
}
