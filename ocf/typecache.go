package ocf

import "github.com/modern-go/reflect2"

// typeCache remembers the value built for the most recent Go type.
type typeCache[T any] struct {
	rtype uintptr
	val   T
	set   bool
}

func (c *typeCache[T]) get(v any, build func(reflect2.Type) (T, error)) (T, error) {
	rtype := reflect2.RTypeOf(v)
	if c.set && c.rtype == rtype {
		return c.val, nil
	}

	val, err := build(reflect2.TypeOf(v))
	if err != nil {
		return val, err
	}
	c.rtype, c.val, c.set = rtype, val, true
	return val, nil
}
