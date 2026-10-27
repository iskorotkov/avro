package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExecuteFromFile(t *testing.T) {
	dir := t.TempDir()
	c := parseFlags([]string{
		"-from", "../../report/pipeline/testdata/raw.txt",
		"-out", filepath.Join(dir, "RESULTS.md"),
		"-charts", filepath.Join(dir, "results", "charts"),
	})
	if err := execute(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	md, err := os.ReadFile(c.out)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(md), "![Summary](results/charts/summary.svg)") {
		t.Error("summary chart not linked relative to the markdown file")
	}
	if _, err := os.Stat(filepath.Join(c.charts, "summary.svg")); err != nil {
		t.Error(err)
	}
}

func TestChartsRef(t *testing.T) {
	if got := chartsRef("RESULTS.md", "results/charts"); got != "results/charts" {
		t.Errorf("got %q", got)
	}
}
