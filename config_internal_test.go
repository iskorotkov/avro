package avro

import (
	"testing"

	"github.com/modern-go/reflect2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Freeze(t *testing.T) {
	api := Config{
		TagKey:      "test",
		BlockLength: 2,
	}.Freeze()
	cfg := api.(*frozenConfig)

	assert.Equal(t, "test", cfg.getTagKey())
	assert.Equal(t, 2, cfg.getBlockLength())
}

func TestConfig_ReusesDecoders(t *testing.T) {
	type testObj struct {
		A int64 `avro:"a"`
	}

	api := Config{
		TagKey:      "test",
		BlockLength: 2,
	}.Freeze()
	cfg := api.(*frozenConfig)

	schema := MustParse(`{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"}
	]
}`)
	typ := reflect2.TypeOfPtr(&testObj{})

	dec1 := cfg.DecoderOf(schema, typ)
	dec2 := cfg.DecoderOf(schema, typ)

	assert.Same(t, dec1, dec2)
}

func TestConfig_ReusesDecoders_WithWriterFingerprint(t *testing.T) {
	type testObj struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	sch := `{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "long"},
			{"name": "b", "type": "string", "default": "foo"}
		]
	}`
	typ := reflect2.TypeOfPtr(&testObj{})

	api := Config{
		TagKey:      "test",
		BlockLength: 2,
	}.Freeze()
	cfg := api.(*frozenConfig)

	schema1 := MustParse(sch)
	schema2 := MustParse(sch)
	fp := [32]byte{1, 2, 3}
	schema2.(*RecordSchema).writerFingerprint = &fp

	dec1 := cfg.DecoderOf(schema1, typ)
	dec2 := cfg.DecoderOf(schema2, typ)

	assert.NotSame(t, dec1, dec2)
}

func TestConfig_ReusesDecoders_WithEnum(t *testing.T) {
	sch := `{
		"type": "enum",
		"name": "test.enum",
		"symbols": ["foo"],
		"default": "foo"
	}`
	typ := reflect2.TypeOfPtr(new(string))

	api := Config{
		TagKey:      "test",
		BlockLength: 2,
	}.Freeze()
	cfg := api.(*frozenConfig)

	schema1 := MustParse(sch)
	schema2 := MustParse(sch)
	schema2.(*EnumSchema).encodedSymbols = []string{"foo", "bar"}
	fp := schema1.Fingerprint()
	schema2.(*EnumSchema).writerFingerprint = &fp

	dec1 := cfg.DecoderOf(schema1, typ)
	dec2 := cfg.DecoderOf(schema2, typ)

	assert.NotSame(t, dec1, dec2)
}

func TestConfig_DisableCache_DoesNotReuseDecoders(t *testing.T) {
	type testObj struct {
		A int64 `avro:"a"`
	}

	api := Config{
		TagKey:         "test",
		BlockLength:    2,
		DisableCaching: true,
	}.Freeze()
	cfg := api.(*frozenConfig)

	schema := MustParse(`{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"}
	]
}`)
	typ := reflect2.TypeOfPtr(&testObj{})

	dec1 := cfg.DecoderOf(schema, typ)
	dec2 := cfg.DecoderOf(schema, typ)

	assert.NotSame(t, dec1, dec2)
}

func TestConfig_ReusesEncoders(t *testing.T) {
	type testObj struct {
		A int64 `avro:"a"`
	}

	api := Config{
		TagKey:      "test",
		BlockLength: 2,
	}.Freeze()
	cfg := api.(*frozenConfig)

	schema := MustParse(`{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"}
	]
}`)
	typ := reflect2.TypeOfPtr(testObj{})

	enc1 := cfg.EncoderOf(schema, typ)
	enc2 := cfg.EncoderOf(schema, typ)

	assert.Same(t, enc1, enc2)
}

func TestConfig_DisableCache_DoesNotReuseEncoders(t *testing.T) {
	type testObj struct {
		A int64 `avro:"a"`
	}

	api := Config{
		TagKey:         "test",
		BlockLength:    2,
		DisableCaching: true,
	}.Freeze()
	cfg := api.(*frozenConfig)

	schema := MustParse(`{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"}
	]
}`)
	typ := reflect2.TypeOfPtr(testObj{})

	enc1 := cfg.EncoderOf(schema, typ)
	enc2 := cfg.EncoderOf(schema, typ)

	assert.NotSame(t, enc1, enc2)
}

func TestConfig_MaxCodecCacheSizeCachesNewCodecsWhenFull(t *testing.T) {
	api := Config{MaxCodecCacheSize: 1}.Freeze()
	cfg := api.(*frozenConfig)

	for _, s := range []string{`"long"`, `"int"`} {
		_, err := api.Marshal(MustParse(s), 1)
		assert.NoError(t, err)
	}
	schema := MustParse(`"float"`)
	_, err := api.Marshal(schema, float32(1))
	assert.NoError(t, err)

	assert.Equal(t, int64(1), cfg.encoderCache.size.Load())
	assert.NotNil(t, cfg.getEncoderFromCache(schema.CacheFingerprint(), reflect2.TypeOf(float32(1)).RType()))
}

func TestConfig_MaxCodecCacheSizeWithRecursiveSchema(t *testing.T) {
	type node struct {
		Value int   `avro:"value"`
		Next  *node `avro:"next"`
	}
	api := Config{MaxCodecCacheSize: 1}.Freeze()
	cfg := api.(*frozenConfig)
	list := MustParse(`{"type":"record","name":"Node","namespace":"cache.recursive","fields":[
		{"name":"value","type":"int"},
		{"name":"next","type":["null","Node"]}
	]}`)
	want := []byte{0x02, 0x02, 0x04, 0x02, 0x06, 0x00}

	for range 3 {
		data, err := api.Marshal(list, node{Value: 1, Next: &node{Value: 2, Next: &node{Value: 3}}})
		require.NoError(t, err)
		assert.Equal(t, want, data)

		_, err = api.Marshal(MustParse(`"long"`), int64(1))
		require.NoError(t, err)

		var got node
		require.NoError(t, api.Unmarshal(list, data, &got))
		assert.Equal(t, node{Value: 1, Next: &node{Value: 2, Next: &node{Value: 3}}}, got)

		var generic map[string]any
		require.NoError(t, api.Unmarshal(list, data, &generic))
		third := map[string]any{"value": 3, "next": nil}
		second := map[string]any{"value": 2, "next": map[string]any{"cache.recursive.Node": third}}
		assert.Equal(t, map[string]any{"value": 1, "next": map[string]any{"cache.recursive.Node": second}}, generic)

		assert.Equal(t, int64(1), cfg.encoderCache.size.Load())
		assert.Equal(t, int64(1), cfg.decoderCache.size.Load())
	}
}
