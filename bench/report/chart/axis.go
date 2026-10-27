package chart

import "math"

// axis maps percentages onto horizontal pixels.
type axis struct {
	left, width float64
	max         float64
	step        float64
}

// newAxis picks a round maximum covering maxPct (at least 120) and a tick step.
func newAxis(left, width, maxPct float64) axis {
	maxPct = math.Max(maxPct, 120)
	step := niceStep(maxPct / 5)
	return axis{left: left, width: width, max: math.Ceil(maxPct/step) * step, step: step}
}

func (a axis) x(pct float64) float64 { return a.left + pct/a.max*a.width }

func (a axis) ticks() []float64 {
	var out []float64
	for v := 0.0; v <= a.max+a.step/2; v += a.step {
		out = append(out, v)
	}
	return out
}

func niceStep(raw float64) float64 {
	pow := math.Pow(10, math.Floor(math.Log10(raw)))
	for _, m := range []float64{1, 2, 2.5, 5, 10} {
		if m*pow >= raw {
			return m * pow
		}
	}
	return 10 * pow
}

// finite reports whether v can be drawn.
func finite(v float64) bool { return !math.IsInf(v, 0) && !math.IsNaN(v) }
