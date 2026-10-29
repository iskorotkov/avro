package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/internal/buildversion"
)

type config struct {
	Verbose bool
	Version bool
}

func main() {
	os.Exit(realMain(os.Args, os.Stdout, os.Stderr))
}

func realMain(args []string, stdout, stderr io.Writer) int {
	var cfg config
	flgs := flag.NewFlagSet("avrosv", flag.ContinueOnError)
	flgs.SetOutput(stderr)
	flgs.BoolVar(&cfg.Verbose, "v", false, "Verbose output (dump final parsed schema).")
	flgs.BoolVar(&cfg.Version, "version", false, "Print the version and exit.")
	flgs.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "Usage: avrosv [options] schemas")
		_, _ = fmt.Fprintln(stderr, "Options:")
		flgs.PrintDefaults()
		_, _ = fmt.Fprintln(stderr, "\nSchemas are processed in the order they appear.")
	}
	if err := flgs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if cfg.Version {
		_, _ = fmt.Fprintln(stdout, "avrosv "+buildversion.Get())
		return 0
	}
	if flgs.NArg() < 1 {
		_, _ = fmt.Fprintln(stderr, "Error: at least one schema is required")
		return 1
	}

	schema, err := avro.ParseFiles(flgs.Args()...)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "Error: %v\n", err)
		return 2
	}

	if cfg.Verbose {
		_, _ = fmt.Fprintln(stdout, schema)
	}

	return 0
}
