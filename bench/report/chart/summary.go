package chart

import (
	"fmt"

	"github.com/iskorotkov/avro/bench/report/stats"
)

const (
	groupH    = 52.0
	sumBarH   = 14.0
	sumBarGap = 3.0
)

// Summary renders per-format geometric-mean relative encode and decode times as grouped bars.
func Summary(rows []stats.SummaryRow, baseline string) []byte {
	maxPct := 0.0
	for _, r := range rows {
		for _, g := range []stats.Geo{r.Encode, r.Decode} {
			if g.OK {
				maxPct = max(maxPct, g.Ratio*100, finiteOr(g.Hi*100, 0))
			}
		}
	}
	ax := newAxis(left, width-left-right, maxPct)
	height := top + 10 + groupH*float64(len(rows)) + axisH + 10
	c := &canvas{w: int(width), h: int(height)}

	title := fmt.Sprintf("Geometric mean over datasets: time per op relative to %s (lower is better)", baseline)
	c.text(16, 24, "start", "title", title)
	c.rect(left, 36, 12, 12, "base")
	c.text(left+18, 46, "start", "muted", "encode")
	c.rect(left+90, 36, 12, 12, "alt")
	c.text(left+108, 46, "start", "muted", "decode")
	c.text(left+190, 46, "start", "muted", "whiskers: format 95% CI only; dashed: 100%")

	plotTop := top + 10
	plotBottom := plotTop + groupH*float64(len(rows))
	drawGrid(c, ax, plotTop, plotBottom)
	for i, r := range rows {
		y := plotTop + groupH*float64(i)
		c.text(left-10, y+groupH/2+4, "end", rowClass(r.Baseline), r.Format)
		drawGeo(c, ax, r.Encode, y+8, "base")
		drawGeo(c, ax, r.Decode, y+8+sumBarH+sumBarGap, "alt")
	}
	drawAxisLabels(c, ax, plotBottom)

	desc := fmt.Sprintf("Grouped horizontal bars of geometric-mean relative encode and decode time per format versus %s.", baseline)
	return c.svg(title, desc)
}

func drawGeo(c *canvas, ax axis, g stats.Geo, y float64, class string) {
	mid := y + sumBarH/2
	if !g.OK {
		c.text(ax.x(0)+6, mid+4, "start", "muted", "—")
		return
	}
	end := ax.x(g.Ratio * 100)
	c.rect(ax.x(0), y, end-ax.x(0), sumBarH, class)
	labelX := end
	if finite(g.Lo) && finite(g.Hi) {
		lo, hi := ax.x(min(g.Lo*100, ax.max)), ax.x(min(g.Hi*100, ax.max))
		c.whisker(lo, hi, mid)
		labelX = max(labelX, hi)
	}
	c.text(labelX+6, mid+4, "start", "", percentLabel(g.Ratio))
}
