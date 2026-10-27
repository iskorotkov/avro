// Package gogenavro adapts github.com/actgardner/gogen-avro types generated from the dataset schemas.
package gogenavro

//go:generate go tool gogen-avro --package=superhero superhero ../../datasets/superhero/schema.avsc
//go:generate go tool gogen-avro --package=nyctaxi nyctaxi ../../datasets/nyctaxi/schema.avsc

import (
	"bytes"
	"io"

	"github.com/actgardner/gogen-avro/v10/compiler"
	"github.com/actgardner/gogen-avro/v10/container"
	"github.com/actgardner/gogen-avro/v10/vm"
	"github.com/actgardner/gogen-avro/v10/vm/types"
	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/datasets/nyctaxi"
	"github.com/iskorotkov/avro/bench/datasets/superhero"
	"github.com/iskorotkov/avro/bench/format"
)

// Name is the format name.
const Name = "gogenavro"

// Same block length as the other adapters.
const blockLength = 100

// Format binds datasets that have generated types in this package.
type Format struct{}

// Name implements format.Format.
func (Format) Name() string { return Name }

// Bind implements format.Format.
func (Format) Bind(ds dataset.Dataset) (*format.Subject, error) {
	switch ds.Name() {
	case superhero.Name:
		return bind(ds, fromSuperhero)
	case nyctaxi.Name:
		return bind(ds, fromTrip)
	default:
		return nil, format.ErrUnsupported
	}
}

// record is a pointer to a generated record type.
type record[T any] interface {
	*T
	container.AvroRecord
	types.Field
}

func bind[S, T any, P record[T]](ds dataset.Dataset, convert func(*S) P) (*format.Subject, error) {
	values, err := dataset.Convert(ds, convert)
	if err != nil {
		return nil, err
	}

	schema := P(new(T)).Schema()
	program, err := compile(schema, schema)
	if err != nil {
		return nil, err
	}

	return &format.Subject{
		Codec: &codec{program: program},
		Batch: &format.StreamBatch{
			NewEncoder: func(w io.Writer) format.Encoder { return encoder{w} },
			NewDecoder: func(r io.Reader) format.Decoder { return decoder{r, program} },
		},
		Container: &format.StreamContainer{
			Codecs: []string{string(container.Null), string(container.Deflate), string(container.Snappy)},
			NewEncoder: func(w io.Writer, compression string) (format.EncodeCloser, error) {
				cw, err := container.NewWriter(w, container.Codec(compression), blockLength, schema)
				return containerEncoder{cw}, err
			},
			NewDecoder: func(r io.Reader) (format.Decoder, error) {
				cr, err := container.NewReader(r)
				if err != nil {
					return nil, err
				}
				p, err := compile(string(cr.AvroContainerSchema()), schema)

				return decoder{cr, p}, err
			},
		},
		Values: values,
		New:    func() any { return P(new(T)) },
	}, nil
}

func compile(writerSchema, readerSchema string) (*vm.Program, error) {
	return compiler.CompileSchemaBytes([]byte(writerSchema), []byte(readerSchema))
}

// codec decodes with a program compiled once, unlike the generated Deserialize functions.
type codec struct {
	program *vm.Program
	buf     bytes.Buffer
}

func (c *codec) Marshal(v any) ([]byte, error) {
	c.buf.Reset()
	if err := v.(container.AvroRecord).Serialize(&c.buf); err != nil {
		return nil, err
	}

	return c.buf.Bytes(), nil
}

func (c *codec) Unmarshal(data []byte, v any) error {
	return vm.Eval(bytes.NewReader(data), c.program, v.(types.Field))
}

type encoder struct {
	w io.Writer
}

func (e encoder) Encode(v any) error { return v.(container.AvroRecord).Serialize(e.w) }

type decoder struct {
	r       io.Reader
	program *vm.Program
}

func (d decoder) Decode(v any) error { return vm.Eval(d.r, d.program, v.(types.Field)) }

// containerEncoder flushes the last block on Close, as the library's writer has no Close.
type containerEncoder struct {
	w *container.Writer
}

func (e containerEncoder) Encode(v any) error { return e.w.WriteRecord(v.(container.AvroRecord)) }

func (e containerEncoder) Close() error { return e.w.Flush() }
