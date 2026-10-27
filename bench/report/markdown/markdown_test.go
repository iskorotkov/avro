package markdown

import "testing"

func TestPercent(t *testing.T) {
	for in, want := range map[float64]string{1: "100%", 0.874: "87%", 0.056: "5.6%", 3.126: "313%"} {
		if got := percent(in); got != want {
			t.Errorf("percent(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestUnits(t *testing.T) {
	for got, want := range map[string]string{
		seconds(1.5e-3): "1.500ms",
		seconds(2.5e-6): "2.500µs",
		bytes(2048):     "2.000KiB",
		bytes(0):        "0B",
		byteRate(150e6): "150.0MB/s",
	} {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}
