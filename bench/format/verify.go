package format

import (
	"bytes"
	"fmt"

	"github.com/google/go-cmp/cmp"
)

// Verify checks that every supported operation of s round-trips all values.
func Verify(s *Subject) error {
	if err := verifyCodec(s); err != nil {
		return fmt.Errorf("codec: %w", err)
	}
	if s.Batch != nil {
		if err := verifyBatch(s); err != nil {
			return fmt.Errorf("batch: %w", err)
		}
	}
	if s.Container != nil {
		for _, c := range s.Container.Compressions() {
			if err := verifyContainer(s, c); err != nil {
				return fmt.Errorf("container %s: %w", c, err)
			}
		}
	}

	return nil
}

func verifyCodec(s *Subject) error {
	for i, v := range s.Values {
		data, err := s.Codec.Marshal(v)
		if err != nil {
			return fmt.Errorf("marshal value %d: %w", i, err)
		}

		got := s.New()
		if err := s.Codec.Unmarshal(data, got); err != nil {
			return fmt.Errorf("unmarshal value %d: %w", i, err)
		}
		if !s.equal(v, got) {
			return fmt.Errorf("value %d differs after round trip%s", i, s.diff(v, got))
		}
	}

	return nil
}

func verifyBatch(s *Subject) error {
	data, err := s.Batch.MarshalBatch(s.Values)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	got := s.NewValues(len(s.Values))
	if err := s.Batch.UnmarshalBatch(data, got); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	return s.equalAll(got)
}

func verifyContainer(s *Subject, compression string) error {
	var buf bytes.Buffer
	if err := s.Container.WriteContainer(&buf, compression, s.Values); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	got := s.NewValues(len(s.Values))
	if err := s.Container.ReadContainer(&buf, got); err != nil {
		return fmt.Errorf("read: %w", err)
	}

	return s.equalAll(got)
}

func (s *Subject) equalAll(got []any) error {
	for i, v := range s.Values {
		if !s.equal(v, got[i]) {
			return fmt.Errorf("value %d differs after round trip", i)
		}
	}

	return nil
}

func (s *Subject) diff(a, b any) string {
	if s.Equal != nil {
		return ""
	}

	return ": " + cmp.Diff(a, b)
}

func (s *Subject) equal(a, b any) bool {
	if s.Equal != nil {
		return s.Equal(a, b)
	}

	return cmp.Equal(a, b)
}
