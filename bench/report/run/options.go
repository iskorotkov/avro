// Package run builds the suite test binary, runs it in rotating rounds and writes the raw results file.
package run

import (
	"fmt"
	"strings"
)

// Options configures a benchmark run; paths are absolute or relative to Dir.
type Options struct {
	// Dir is the bench module directory.
	Dir string
	// Package is the suite package to build, e.g. ./suite.
	Package string
	// Count is the number of rounds.
	Count int
	// Procs is used for GOMAXPROCS and -test.cpu.
	Procs     int
	Benchtime string
	Dataset   string
	Op        string
	Format    string
	// Tags are comma-separated build tags passed to go test -tags.
	Tags string
	// GoExperiment is the GOEXPERIMENT value the suite is built with.
	GoExperiment string
	// DataDir is passed as -suite.data when set.
	DataDir string
	// CPUProfileDir receives one profile per round when set.
	CPUProfileDir string
	// ResultsDir receives the dated file and latest.txt.
	ResultsDir string
}

// BenchRegex builds the -test.bench pattern; Go matches each slash-separated level on its own.
func (o Options) BenchRegex() string {
	return fmt.Sprintf("^BenchmarkSuite$/^dataset=(%s)$/^op=(%s)$/^format=(%s)$",
		orAny(o.Dataset), orAny(o.Op), orAny(o.Format))
}

// FlagsLine renders the effective report flags for the raw file header.
func (o Options) FlagsLine() string {
	parts := []string{
		fmt.Sprintf("-count=%d", o.Count),
		"-benchtime=" + o.Benchtime,
		fmt.Sprintf("-procs=%d", o.Procs),
		"-dataset=" + orAny(o.Dataset),
		"-op=" + orAny(o.Op),
		"-format=" + orAny(o.Format),
	}
	if o.Tags != "" {
		parts = append(parts, "-tags="+o.Tags)
	}
	if o.GoExperiment != "" {
		parts = append(parts, "-goexperiment="+o.GoExperiment)
	}
	if o.DataDir != "" {
		parts = append(parts, "-data="+o.DataDir)
	}
	if o.CPUProfileDir != "" {
		parts = append(parts, "-cpuprofile="+o.CPUProfileDir)
	}
	return strings.Join(parts, " ")
}

func orAny(s string) string {
	if s == "" {
		return ".*"
	}
	return s
}
