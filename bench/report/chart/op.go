package chart

import (
	"fmt"

	"github.com/iskorotkov/avro/bench/report/stats"
)

const (
	width   = 720.0
	left    = 150.0
	right   = 80.0
	top     = 56.0
	rowH    = 30.0
	barH    = 16.0
	axisH   = 26.0
	legendH = 34.0
)

// Op renders relative time per op of every format in one dataset/op as horizontal bars.
func Op(dataset string, op stats.Op, baseline string) []byte {
	maxPct := 0.0
	for _, r := range op.Rows {
		if rel, ok := r.Time(); ok {
			maxPct = max(maxPct, rel.Ratio*100, finiteOr(rel.Hi*100, 0))
		}
	}
	ax := newAxis(left, width-left-right, maxPct)
	height := top + rowH*float64(len(op.Rows)) + axisH + legendH
	c := &canvas{w: int(width), h: int(height)}

	title := fmt.Sprintf("%s / %s: time per op relative to %s (lower is better)", dataset, op.Name, baseline)
	c.text(16, 24, "start", "title", title)
	drawGrid(c, ax, top, top+rowH*float64(len(op.Rows)))

	for i, r := range op.Rows {
		y := top + rowH*float64(i)
		c.text(left-10, y+rowH/2+4, "end", rowClass(r.Baseline), r.Format)
		drawBar(c, ax, r, y)
	}
	axisY := top + rowH*float64(len(op.Rows))
	drawAxisLabels(c, ax, axisY)
	drawLegend(c, axisY+axisH+14, baseline)

	desc := fmt.Sprintf("Horizontal bars of median time per op for %s on %s, as a percentage of %s.", op.Name, dataset, baseline)
	return c.svg(title, desc)
}

func rowClass(baseline bool) string {
	if baseline {
		return "bold"
	}
	return ""
}

func drawBar(c *canvas, ax axis, r stats.Row, y float64) {
	rel, ok := r.Time()
	if !ok {
		c.text(ax.x(0)+6, y+rowH/2+4, "start", "muted", "—")
		return
	}
	class := "other"
	if r.Baseline {
		class = "base"
	}
	mid := y + rowH/2
	end := ax.x(rel.Ratio * 100)
	c.rect(ax.x(0), mid-barH/2, end-ax.x(0), barH, class)
	labelX := end
	if finite(rel.Lo) && finite(rel.Hi) {
		lo, hi := ax.x(min(rel.Lo*100, ax.max)), ax.x(min(rel.Hi*100, ax.max))
		c.whisker(lo, hi, mid)
		labelX = max(labelX, hi)
	}
	c.text(labelX+6, mid+4, "start", "", percentLabel(rel.Ratio))
}

func drawGrid(c *canvas, ax axis, y1, y2 float64) {
	for _, t := range ax.ticks() {
		c.line(ax.x(t), y1, ax.x(t), y2, "grid")
	}
	c.line(ax.x(100), y1-4, ax.x(100), y2, "ref")
}

func drawAxisLabels(c *canvas, ax axis, y float64) {
	for _, t := range ax.ticks() {
		c.text(ax.x(t), y+16, "middle", "muted", fmt.Sprintf("%.0f%%", t))
	}
}

func drawLegend(c *canvas, y float64, baseline string) {
	c.rect(left, y-10, 12, 12, "base")
	c.text(left+18, y, "start", "muted", baseline+" (baseline)")
	c.rect(left+150, y-10, 12, 12, "other")
	c.text(left+168, y, "start", "muted", "other formats")
	c.text(left+290, y, "start", "muted", "whiskers: format 95% CI only; dashed: 100%")
}

func finiteOr(v, fallback float64) float64 {
	if finite(v) {
		return v
	}
	return fallback
}
