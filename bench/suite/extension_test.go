package suite_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/iskorotkov/avro/bench/format"
	"github.com/iskorotkov/avro/bench/suite"
)

type fakeExtension struct {
	err error
}

func (fakeExtension) Ops(suite.Case) []suite.Op {
	return []suite.Op{{Name: "extra", Run: func(*testing.B, *format.Subject) {}}}
}

func (f fakeExtension) Verify(suite.Case) error { return f.err }

type rawString struct{}

func (rawString) Marshal(v any) ([]byte, error) { return []byte(*v.(*string)), nil }

func (rawString) Unmarshal(data []byte, v any) error {
	*v.(*string) = string(data)
	return nil
}

func stringCase() suite.Case {
	a, b := "a", "b"
	return suite.Case{Subject: &format.Subject{Codec: rawString{}, Values: []any{&a, &b}, New: func() any { return new(string) }}}
}

func TestCaseOpsAppendsExtensionOps(t *testing.T) {
	var names []string
	for _, op := range suite.CaseOps(stringCase(), []suite.Extension{fakeExtension{}}) {
		names = append(names, op.Name)
	}
	if !slices.Equal(names, []string{"encode", "decode", "extra"}) {
		t.Errorf("ops = %v", names)
	}
}

func TestVerifyRunsExtensions(t *testing.T) {
	c := stringCase()
	if err := suite.Verify(c, []suite.Extension{fakeExtension{}}); err != nil {
		t.Fatal(err)
	}
	want := errors.New("broken")
	if err := suite.Verify(c, []suite.Extension{fakeExtension{err: want}}); !errors.Is(err, want) {
		t.Errorf("err = %v, want %v", err, want)
	}
}
