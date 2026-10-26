package avro

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
)

// ErrUnknownField is returned when Config.DisallowUnknownFields is set and a struct and record schema do not match.
var ErrUnknownField = errors.New("avro: unknown field")

// checkStructFields enforces Config.DisallowUnknownFields for a struct codec.
func (c *frozenConfig) checkStructFields(rec *RecordSchema, desc *structDescriptor, withAliases bool) error {
	// The null placeholder stands for a missing value, so it has no fields to match.
	if !c.config.DisallowUnknownFields || desc.Type.Type1() == reflect.TypeFor[null]() {
		return nil
	}

	for _, field := range rec.Fields() {
		if field.action == FieldIgnore || hasStructField(desc, field, withAliases) {
			continue
		}
		return fmt.Errorf("%w: record %s field %q has no matching field in %s", ErrUnknownField, rec.FullName(), field.Name(), desc.Type)
	}

	for _, sf := range desc.Fields {
		if sf.Name == "-" || hasRecordField(rec, sf.Name, withAliases) {
			continue
		}
		return fmt.Errorf("%w: %s field %q has no matching field in record %s", ErrUnknownField, desc.Type, sf.Name, rec.FullName())
	}

	return nil
}

// failBuild keeps the first error that must fail the whole decoder.
func (d *decoderContext) failBuild(err error) {
	if d.buildErr == nil {
		d.buildErr = err
	}
}

// failBuild keeps the first error that must fail the whole encoder.
func (e *encoderContext) failBuild(err error) {
	if e.buildErr == nil {
		e.buildErr = err
	}
}

func hasStructField(desc *structDescriptor, field *Field, withAliases bool) bool {
	if desc.Fields.Get(field.Name()) != nil {
		return true
	}
	if !withAliases {
		return false
	}
	return slices.ContainsFunc(field.Aliases(), func(alias string) bool { return desc.Fields.Get(alias) != nil })
}

func hasRecordField(rec *RecordSchema, name string, withAliases bool) bool {
	return slices.ContainsFunc(rec.Fields(), func(f *Field) bool {
		if f.action == FieldIgnore {
			return false
		}
		return f.Name() == name || withAliases && slices.Contains(f.Aliases(), name)
	})
}
