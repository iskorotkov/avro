package chart

import (
	"encoding/xml"
	"strings"
	"testing"

	"github.com/iskorotkov/avro/bench/report/stats"
)

func TestOpEscapesAndIsValidXML(t *testing.T) {
	rows := []stats.Row{
		{Format: "avro", Baseline: true, Est: map[stats.Metric]stats.Estimate{}, Rel: map[stats.Metric]stats.Relative{stats.SecPerRec: {Ratio: 1, Lo: 0.9, Hi: 1.1}}},
		{Format: `<b>&"x"`, Rel: map[stats.Metric]stats.Relative{stats.SecPerRec: {Ratio: 0.5, Lo: 0.4, Hi: 0.6}}},
		{Format: "missing"},
	}
	svg := string(Op("ds", stats.Op{Name: "encode", Rows: rows}, "avro"))
	if strings.Contains(svg, "<b>") {
		t.Error("format name not escaped")
	}
	if err := xml.Unmarshal([]byte(svg), new(struct{})); err != nil {
		t.Errorf("invalid xml: %v", err)
	}
	if !strings.Contains(svg, "50%") {
		t.Error("value label missing")
	}
}

func TestNewAxisCoversMax(t *testing.T) {
	ax := newAxis(0, 100, 387)
	if ax.max < 387 || ax.max > 600 {
		t.Errorf("max = %v", ax.max)
	}
	if got := newAxis(0, 100, 10).max; got < 120 {
		t.Errorf("max = %v, want at least 120", got)
	}
}
