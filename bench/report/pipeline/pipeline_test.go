package pipeline_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/iskorotkov/avro/bench/report/jmh"
	"github.com/iskorotkov/avro/bench/report/pipeline"
	"github.com/iskorotkov/avro/bench/report/raw"
)

func generate(t *testing.T) pipeline.Output {
	t.Helper()
	results, err := raw.ReadFile("testdata/raw.txt")
	if err != nil {
		t.Fatal(err)
	}
	java, err := jmh.Parse(strings.NewReader(`[{"benchmark":"b","mode":"avgt","params":{"dataset":"superhero","op":"encode"},
		"primaryMetric":{"score":1,"scoreUnit":"ms/op","rawData":[[1,1.1,0.9,1,1,1]]}}]`))
	if err != nil {
		t.Fatal(err)
	}
	out, err := pipeline.Generate(append(results, java...), pipeline.Options{Baseline: "avro", Noise: 0.05, ChartsRef: "results/charts"})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestGenerate(t *testing.T) {
	out := generate(t)
	md := string(out.Markdown)
	for _, want := range []string{
		"| CPU | Example CPU @ 3.00GHz |",
		"![Summary](results/charts/summary.svg)",
		"## nyctaxi",
		"### container-write-zstd",
		"![superhero encode](results/charts/superhero-encode.svg)",
		"| **avro** |",
		"java-avro",
		"100%",
		"⚠",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("markdown lacks %q", want)
		}
	}
	if strings.Index(md, "### encode") > strings.Index(md, "### decode") {
		t.Error("encode must precede decode")
	}
	// batch-encode on nyctaxi has no baseline: relative columns are dashes and no chart is embedded.
	if strings.Contains(md, "nyctaxi-batch-encode.svg") {
		t.Error("op without baseline must not embed a chart")
	}

	names := map[string]bool{}
	for _, f := range out.Charts {
		names[f.Name] = true
		if !bytes.HasPrefix(f.Data, []byte("<svg")) || !bytes.Contains(f.Data, []byte("prefers-color-scheme: dark")) {
			t.Errorf("%s is not a themed svg", f.Name)
		}
	}
	for _, want := range []string{"summary.svg", "superhero-encode.svg", "nyctaxi-container-write-zstd.svg"} {
		if !names[want] {
			t.Errorf("missing chart %s", want)
		}
	}
}

func TestGenerateDeterministic(t *testing.T) {
	a, b := generate(t), generate(t)
	if !bytes.Equal(a.Markdown, b.Markdown) {
		t.Error("markdown differs between runs")
	}
	for i := range a.Charts {
		if !bytes.Equal(a.Charts[i].Data, b.Charts[i].Data) {
			t.Errorf("chart %s differs between runs", a.Charts[i].Name)
		}
	}
}
