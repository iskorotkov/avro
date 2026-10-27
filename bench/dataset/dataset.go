// Package dataset defines the benchmark dataset abstraction and its sample storage.
package dataset

// Dataset is a named, in-memory set of records sharing one Avro schema.
type Dataset interface {
	// Name is the stable identifier used in benchmark names.
	Name() string
	// Schema is the Avro schema of a record in JSON form.
	Schema() string
	// Records returns pointers to the canonical records.
	Records() []any
	// New returns a pointer to a new zero canonical record.
	New() any
}

// Source is a pinned download of a dataset's full data.
type Source struct {
	// URL is where the file is downloaded from.
	URL string
	// SHA256 is the hex-encoded checksum of the downloaded file.
	SHA256 string
	// File is the name the download is stored under in the dataset's data directory.
	File string
}

// Provider loads a dataset from its committed sample or its full data.
type Provider interface {
	// Name is the dataset name.
	Name() string
	// Sources lists the full data downloads; empty when only the sample exists.
	Sources() []Source
	// Load reads the committed sample, or the full data when dir is non-empty, keeping the first rows records when rows is positive.
	Load(dir string, rows int) (Dataset, error)
}

// Of is a Dataset of canonical records of type T.
type Of[T any] struct {
	name    string
	schema  string
	records []any
}

// New wraps records of type T into a Dataset.
func New[T any](name, schema string, records []T) *Of[T] {
	ptrs := make([]any, len(records))
	for i := range records {
		ptrs[i] = &records[i]
	}

	return &Of[T]{name: name, schema: schema, records: ptrs}
}

// Name implements Dataset.
func (d *Of[T]) Name() string { return d.name }

// Schema implements Dataset.
func (d *Of[T]) Schema() string { return d.schema }

// Records implements Dataset.
func (d *Of[T]) Records() []any { return d.records }

// New implements Dataset.
func (d *Of[T]) New() any { return new(T) }
