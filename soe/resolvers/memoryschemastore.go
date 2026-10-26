package resolvers

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/soe"
)

// ErrFingerprintCollision is returned when a different schema already holds the fingerprint.
var ErrFingerprintCollision = errors.New("schema fingerprint collision")

// MemorySchemaStore is a basic in-memory schema store and resolver
// implementation.
type MemorySchemaStore struct {
	schemas sync.Map // map[uint64]avro.Schema
}

// NewMemorySchemaStore creates a new MemorySchemaStore.
func NewMemorySchemaStore() *MemorySchemaStore {
	return &MemorySchemaStore{}
}

// AddSchema puts a schema into the registry.
//
// Adding a schema whose fingerprint is already held by a schema with a
// different canonical form returns ErrFingerprintCollision and keeps the
// existing schema.
func (s *MemorySchemaStore) AddSchema(schema avro.Schema) error {
	fp, err := soe.ComputeFingerprint(schema)
	if err != nil {
		return err
	}

	key := keyFromFingerprint(fp)
	existing, loaded := s.schemas.LoadOrStore(key, schema)
	if loaded && existing.(avro.Schema).String() != schema.String() {
		return fmt.Errorf("%w: %x", ErrFingerprintCollision, fp)
	}
	return nil
}

// GetSchema implements SchemaResolver.
func (s *MemorySchemaStore) GetSchema(_ context.Context, fingerprint []byte) (avro.Schema, error) {
	key := keyFromFingerprint(fingerprint)
	schema, ok := s.schemas.Load(key)
	if !ok {
		return nil, fmt.Errorf("%w: %x", soe.ErrUnknownSchema, fingerprint)
	}
	return schema.(avro.Schema), nil
}

// keyFromFingerprint creates an arbitrary uint64 encoding of a schema
// fingerprint, suitable as a map key.
func keyFromFingerprint(fingerprint []byte) uint64 {
	return binary.LittleEndian.Uint64(fingerprint)
}
