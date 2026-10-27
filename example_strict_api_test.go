package avro_test

import (
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/iskorotkov/avro/v2"
)

const exampleUserSchema = `{"type":"record","name":"user","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}`

type exampleUser struct {
	ID   int64  `avro:"id"`
	Name string `avro:"name"`
}

func ExampleAppendMarshal() {
	schema := avro.MustParse(exampleUserSchema)

	buf := make([]byte, 0, 64)
	for _, u := range []exampleUser{{ID: 1, Name: "ann"}, {ID: 2, Name: "bob"}} {
		var err error
		buf, err = avro.AppendMarshal(buf[:0], schema, &u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("% x\n", buf)
	}

	// Output:
	// 02 06 61 6e 6e
	// 04 06 62 6f 62
}

func ExampleAppendMarshalWith() {
	schema := avro.MustParse(exampleUserSchema)
	api := avro.Config{DisallowUnknownFields: true}.Freeze()

	buf, err := avro.AppendMarshalWith(api, []byte{0xff}, schema, &exampleUser{ID: 1, Name: "ann"})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("% x\n", buf)
	// Output: ff 02 06 61 6e 6e
}

func ExampleConfig_disallowUnknownFields() {
	schema := avro.MustParse(exampleUserSchema)
	api := avro.Config{DisallowUnknownFields: true}.Freeze()

	type idOnly struct {
		ID int64 `avro:"id"`
	}
	err := api.Unmarshal(schema, []byte{0x02, 0x06, 'a', 'n', 'n'}, &idOnly{})

	fmt.Println(errors.Is(err, avro.ErrUnknownField))
	// Output: true
}

func ExampleConfig_aliasInput() {
	schema := avro.MustParse(exampleUserSchema)
	api := avro.Config{AliasInput: true}.Freeze()
	data := []byte{0x02, 0x06, 'a', 'n', 'n'}

	var u exampleUser
	if err := api.Unmarshal(schema, data, &u); err != nil {
		log.Fatal(err)
	}
	data[2] = 'A' // u.Name shares memory with data.

	fmt.Println(u.Name)
	// Output: Ann
}

func ExampleSemanticError() {
	schema := avro.MustParse(`{"type":"record","name":"order","fields":[{"name":"buyer","type":` + exampleUserSchema + `}]}`)
	type order struct {
		Buyer exampleUser `avro:"buyer"`
	}

	err := avro.Unmarshal(schema, []byte{0x02, 0x7f}, &order{})

	if semErr, ok := errors.AsType[*avro.SemanticError](err); ok {
		fmt.Println(semErr.Field)
	}
	// Output: Buyer.Name
}

func ExampleShortBufferError() {
	schema := avro.MustParse(exampleUserSchema)

	err := avro.Unmarshal(schema, []byte{0x02}, &exampleUser{})

	if shortErr, ok := errors.AsType[*avro.ShortBufferError](err); ok {
		fmt.Println(shortErr.Have, errors.Is(err, io.ErrUnexpectedEOF))
	}
	// Output: 1 true
}
