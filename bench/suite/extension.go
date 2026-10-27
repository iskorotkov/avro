package suite

import (
	"fmt"

	"github.com/iskorotkov/avro/bench/format"
)

// Extension contributes operations beyond the built-in ones, such as RPC round trips.
type Extension interface {
	// Ops returns the extension's operations for c, or nil when it does not apply to c.
	Ops(c Case) []Op
	// Verify checks that the extension's operations round-trip the values of c.
	Verify(c Case) error
}

// CaseOps returns the built-in operations of c followed by those of every extension.
func CaseOps(c Case, exts []Extension) []Op {
	ops := Ops(c.Subject)
	for _, e := range exts {
		ops = append(ops, e.Ops(c)...)
	}

	return ops
}

// Verify checks that every built-in and extension operation of c round-trips its values.
func Verify(c Case, exts []Extension) error {
	if err := format.Verify(c.Subject); err != nil {
		return err
	}
	for _, e := range exts {
		if err := e.Verify(c); err != nil {
			return fmt.Errorf("%T: %w", e, err)
		}
	}

	return nil
}
