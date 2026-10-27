// Package ghwebhooks provides GitHub webhook event payloads, a nested real-life dataset with many nullable records and URLs.
package ghwebhooks

import (
	_ "embed"
	"fmt"
	"path/filepath"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/iskorotkov/avro/bench/dataset"
)

// Name is the dataset name.
const Name = "ghwebhooks"

const fullFile = "webhooks-examples-7.6.1.tgz"

var (
	//go:embed schema.avsc
	schema string
	//go:embed sample.jsonl.zst
	sample []byte
)

// Equal reports whether two events are equal, treating nil and empty slices and maps alike.
func (e *Event) Equal(other *Event) bool {
	type plain Event

	return cmp.Equal((*plain)(e), (*plain)(other), cmpopts.EquateEmpty())
}

// Provider loads the GitHub webhook payload dataset.
type Provider struct{}

// Name implements dataset.Provider.
func (Provider) Name() string { return Name }

// Sources implements dataset.Provider.
func (Provider) Sources() []dataset.Source {
	return []dataset.Source{{
		URL:    "https://registry.npmjs.org/@octokit/webhooks-examples/-/" + fullFile,
		SHA256: "89e6cb1c6345d32e3dd775617fa54effbdc26c859e514898d7728e95e9738280",
		File:   fullFile,
	}}
}

// Load implements dataset.Provider.
func (Provider) Load(dir string, rows int) (dataset.Dataset, error) {
	records, err := load(dir, rows)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", Name, err)
	}

	return dataset.New(Name, schema, records), nil
}

func load(dir string, rows int) ([]Event, error) {
	if dir == "" {
		events, err := dataset.DecodeJSONL[Event](sample)
		return dataset.Head(events, rows), err
	}

	events, err := ReadPackage(filepath.Join(dir, fullFile), "")

	return dataset.Head(events, rows), err
}
