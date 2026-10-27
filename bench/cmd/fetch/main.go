// Command fetch downloads the full datasets at their pinned URLs and verifies their SHA-256.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"

	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/datasets"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fetch:", err)
		os.Exit(1)
	}
}

func run() error {
	dir := flag.String("dir", "data", "download into `dir`/<dataset>")
	pattern := flag.String("dataset", ".", "fetch datasets whose name matches `regexp`")
	flag.Parse()

	match, err := regexp.Compile(*pattern)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	for _, p := range datasets.All() {
		if !match.MatchString(p.Name()) {
			continue
		}
		for _, src := range p.Sources() {
			fmt.Fprintf(os.Stderr, "%s: %s\n", p.Name(), src.URL)
			if err := dataset.Fetch(ctx, http.DefaultClient, filepath.Join(*dir, p.Name()), src); err != nil {
				return err
			}
		}
	}

	return nil
}
