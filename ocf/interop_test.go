package ocf_test

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"

	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type weather struct {
	Station string `avro:"station" json:"station"`
	Time    int64  `avro:"time" json:"time"`
	Temp    int32  `avro:"temp" json:"temp"`
}

func TestDecoder_ApacheAvroFixtures(t *testing.T) {
	want := readWeatherJSON(t, "testdata/apache/weather.json")
	fixtures := map[string]ocf.CodecName{
		"weather.avro":         ocf.Null,
		"weather-deflate.avro": ocf.Deflate,
		"weather-snappy.avro":  ocf.Snappy,
		"weather-zstd.avro":    ocf.ZStandard,
	}

	for file, codec := range fixtures {
		t.Run(file, func(t *testing.T) {
			f, err := os.Open("testdata/apache/" + file)
			require.NoError(t, err)
			defer f.Close()

			dec, err := ocf.NewDecoder(f, ocf.WithMaxBlockBytes(1<<10), ocf.WithMaxDecompressedBlockBytes(1<<10))
			require.NoError(t, err)
			defer dec.Close()
			var got []weather
			for dec.HasNext() {
				var w weather
				require.NoError(t, dec.Decode(&w))
				got = append(got, w)
			}

			require.NoError(t, dec.Error())
			assert.Equal(t, string(codec), codecOf(dec))
			assert.Equal(t, want, got)
		})
	}
}

func codecOf(dec *ocf.Decoder) string {
	if c := dec.Metadata()["avro.codec"]; len(c) > 0 {
		return string(c)
	}
	return string(ocf.Null)
}

func readWeatherJSON(t *testing.T, path string) []weather {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	var out []weather
	s := bufio.NewScanner(f)
	for s.Scan() {
		var w weather
		require.NoError(t, json.Unmarshal(s.Bytes(), &w))
		out = append(out, w)
	}
	require.NoError(t, s.Err())
	return out
}
