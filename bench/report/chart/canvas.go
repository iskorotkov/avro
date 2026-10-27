// Package chart renders benchmark comparisons as self-contained, theme-aware SVG files.
package chart

import (
	"fmt"
	"html"
	"strings"
)

const style = `<style>
:root{--bg:#fcfcfb;--fg:#0b0b0b;--muted:#52514e;--grid:#e3e2de;--other:#85847e;--base:#2a78d6;--alt:#eb6834}
@media (prefers-color-scheme: dark){:root{--bg:#1a1a19;--fg:#f0efec;--muted:#c3c2b7;--grid:#3a3a38;--other:#8f8e86;--base:#3987e5;--alt:#d95926}}
text{font-family:system-ui,-apple-system,"Segoe UI",sans-serif;font-size:12px;fill:var(--fg)}
.title{font-size:14px;font-weight:600}
.muted{fill:var(--muted)}
.bold{font-weight:600}
.grid{stroke:var(--grid);stroke-width:1}
.ref{stroke:var(--muted);stroke-width:1.5;stroke-dasharray:4 3}
.whisker{stroke:var(--fg);stroke-width:1.5;fill:none}
.bg{fill:var(--bg)}
.other{fill:var(--other)}
.base{fill:var(--base)}
.alt{fill:var(--alt)}
</style>`

// canvas accumulates SVG elements; all text is escaped.
type canvas struct {
	w, h int
	body strings.Builder
}

func (c *canvas) rect(x, y, w, h float64, class string) {
	fmt.Fprintf(&c.body, `<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="3" class="%s"/>`+"\n", x, y, w, h, class)
}

func (c *canvas) line(x1, y1, x2, y2 float64, class string) {
	fmt.Fprintf(&c.body, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" class="%s"/>`+"\n", x1, y1, x2, y2, class)
}

func (c *canvas) text(x, y float64, anchor, class, s string) {
	fmt.Fprintf(&c.body, `<text x="%.1f" y="%.1f" text-anchor="%s" class="%s">%s</text>`+"\n", x, y, anchor, class, html.EscapeString(s))
}

// whisker draws a horizontal CI range with end caps centered on y.
func (c *canvas) whisker(x1, x2, y float64) {
	fmt.Fprintf(&c.body, `<path d="M%.1f %.1fH%.1fM%.1f %.1fV%.1fM%.1f %.1fV%.1f" class="whisker"/>`+"\n",
		x1, y, x2, x1, y-3, y+3, x2, y-3, y+3)
}

func (c *canvas) svg(title, desc string) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d" role="img">`+"\n", c.w, c.h, c.w, c.h)
	fmt.Fprintf(&b, "<title>%s</title>\n<desc>%s</desc>\n", html.EscapeString(title), html.EscapeString(desc))
	b.WriteString(style + "\n")
	fmt.Fprintf(&b, `<rect class="bg" width="%d" height="%d" rx="6"/>`+"\n", c.w, c.h)
	b.WriteString(c.body.String())
	b.WriteString("</svg>\n")
	return []byte(b.String())
}

// percentLabel formats a ratio as a whole percent, with one decimal below 10%.
func percentLabel(ratio float64) string {
	if pct := ratio * 100; pct < 10 {
		return fmt.Sprintf("%.1f%%", pct)
	}
	return fmt.Sprintf("%.0f%%", ratio*100)
}
