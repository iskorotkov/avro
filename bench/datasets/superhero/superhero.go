// Package superhero provides the synthetic superhero dataset with nested records and arrays.
package superhero

import (
	_ "embed"
	"fmt"

	"github.com/iskorotkov/avro/bench/dataset"
)

// Name is the dataset name.
const Name = "superhero"

var (
	//go:embed schema.avsc
	schema string
	//go:embed sample.jsonl.zst
	sample []byte
)

// Superhero is the canonical record.
type Superhero struct {
	ID            int32         `avro:"id"             json:"id"`
	AffiliationID int32         `avro:"affiliation_id" json:"affiliation_id"`
	Name          string        `avro:"name"           json:"name"`
	Life          float32       `avro:"life"           json:"life"`
	Energy        float32       `avro:"energy"         json:"energy"`
	Powers        []*Superpower `avro:"powers"         json:"powers"`
}

// Superpower is a nested record of Superhero.
type Superpower struct {
	ID      int32   `avro:"id"      json:"id"`
	Name    string  `avro:"name"    json:"name"`
	Damage  float32 `avro:"damage"  json:"damage"`
	Energy  float32 `avro:"energy"  json:"energy"`
	Passive bool    `avro:"passive" json:"passive"`
}

// Provider loads the superhero dataset.
type Provider struct{}

// Name implements dataset.Provider.
func (Provider) Name() string { return Name }

// Sources implements dataset.Provider; the dataset is synthetic, so only the sample exists.
func (Provider) Sources() []dataset.Source { return nil }

// Load implements dataset.Provider.
func (Provider) Load(_ string, rows int) (dataset.Dataset, error) {
	records, err := dataset.DecodeJSONL[Superhero](sample)
	if err != nil {
		return nil, fmt.Errorf("%s sample: %w", Name, err)
	}

	return dataset.New(Name, schema, dataset.Head(records, rows)), nil
}
