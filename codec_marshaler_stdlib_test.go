package avro_test

import (
	"bytes"
	"encoding"
	"math/big"
	"net"
	"net/netip"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStdlibTextAppenderMatchesTextMarshaler pins the contract the encoder fast-path relies on:
// for stdlib types implementing both, AppendText output must match MarshalText.
func TestStdlibTextAppenderMatchesTextMarshaler(t *testing.T) {
	type pair struct {
		marshaler encoding.TextMarshaler
		appender  encoding.TextAppender
	}
	mkPair := func(t *testing.T, v any) pair {
		t.Helper()
		tm, ok := v.(encoding.TextMarshaler)
		require.True(t, ok, "%T does not implement encoding.TextMarshaler", v)
		ta, ok := v.(encoding.TextAppender)
		require.True(t, ok, "%T does not implement encoding.TextAppender", v)
		return pair{tm, ta}
	}

	cases := []struct {
		name  string
		value any
	}{
		{"time.Time/zero", time.Time{}},
		{"time.Time/UTC", time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)},
		{"time.Time/Nanos", time.Date(2020, 1, 2, 3, 4, 5, 123456789, time.UTC)},
		{"time.Time/Offset", time.Date(2020, 1, 2, 3, 4, 5, 0, time.FixedZone("+03:00", 3*3600))},

		{"net.IP/v4", net.IPv4(192, 0, 2, 1)},
		{"net.IP/v6", net.ParseIP("2001:db8::1")},
		{"net.IP/zero", net.IP{}},

		{"netip.Addr/v4", netip.MustParseAddr("192.0.2.1")},
		{"netip.Addr/v6", netip.MustParseAddr("2001:db8::1")},
		{"netip.Addr/v6zone", netip.MustParseAddr("fe80::1%eth0")},
		{"netip.Addr/zero", netip.Addr{}},

		{"netip.AddrPort/v4", netip.MustParseAddrPort("192.0.2.1:8080")},
		{"netip.AddrPort/v6", netip.MustParseAddrPort("[2001:db8::1]:443")},

		{"netip.Prefix/v4", netip.MustParsePrefix("192.0.2.0/24")},
		{"netip.Prefix/v6", netip.MustParsePrefix("2001:db8::/32")},

		{"*big.Int/zero", big.NewInt(0)},
		{"*big.Int/positive", big.NewInt(12345678901234)},
		{"*big.Int/negative", big.NewInt(-12345678901234)},
		{"*big.Int/wide", new(big.Int).Lsh(big.NewInt(1), 200)},

		{"*big.Float/pi", big.NewFloat(3.141592653589793)},
		{"*big.Float/negative", big.NewFloat(-2.5)},
		{"*big.Float/zero", big.NewFloat(0)},

		{"*big.Rat/half", big.NewRat(1, 2)},
		{"*big.Rat/approxPi", big.NewRat(355, 113)},
		{"*big.Rat/zero", new(big.Rat)},

		{"*regexp.Regexp/simple", regexp.MustCompile(`^[a-z]+$`)},
		{"*regexp.Regexp/escape", regexp.MustCompile(`\d+\.\d+`)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := mkPair(t, tc.value)

			marshaled, err := p.marshaler.MarshalText()
			require.NoError(t, err, "MarshalText")

			appended, err := p.appender.AppendText(nil)
			require.NoError(t, err, "AppendText(nil)")
			// bytes.Equal: net.IP{}/netip.Addr{} return []byte{} vs nil; the Avro encoder length-prefixes so it's invisible.
			assert.True(t, bytes.Equal(marshaled, appended),
				"AppendText(nil) bytes must equal MarshalText: got %q want %q", appended, marshaled)

			// Shape the encoder actually uses: len=0, cap>0, like w.scratch[:0].
			scratch := make([]byte, 0, 256)
			scratched, err := p.appender.AppendText(scratch)
			require.NoError(t, err, "AppendText(empty-with-cap)")
			assert.True(t, bytes.Equal(marshaled, scratched),
				"AppendText(empty-with-cap) bytes must equal MarshalText: got %q want %q", scratched, marshaled)

			prefix := []byte("PREFIX:")
			prefixed, err := p.appender.AppendText(append([]byte(nil), prefix...))
			require.NoError(t, err, "AppendText(prefix)")
			require.GreaterOrEqual(t, len(prefixed), len(prefix), "AppendText must not shrink the input")
			assert.Equal(t, prefix, prefixed[:len(prefix)], "AppendText must preserve the input prefix")
			assert.True(t, bytes.Equal(marshaled, prefixed[len(prefix):]),
				"AppendText(prefix) tail bytes must equal MarshalText: got %q want %q", prefixed[len(prefix):], marshaled)
		})
	}
}
