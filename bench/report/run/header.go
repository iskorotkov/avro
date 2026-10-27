package run

import (
	"fmt"
	"io"
	"time"
)

// Header describes a run in benchfmt file-configuration lines.
type Header struct {
	Commit     string
	Date       time.Time
	Go         string
	GOMAXPROCS int
	Flags      string
}

// WriteTo writes the header as benchfmt "key: value" lines followed by a blank line.
func (h Header) WriteTo(w io.Writer) (int64, error) {
	n, err := fmt.Fprintf(w, "commit: %s\ndate: %s\ngo: %s\ngomaxprocs: %d\nflags: %s\n\n",
		h.Commit, h.Date.Format(time.RFC3339), h.Go, h.GOMAXPROCS, h.Flags)
	return int64(n), err
}
