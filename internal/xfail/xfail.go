// Package xfail runs test checks against a documented list of expected failures.
package xfail

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
)

// Entry documents one check that is known to fail.
type Entry struct {
	ID       string `json:"id"`
	Reason   string `json:"reason"`
	Tracking string `json:"tracking"`
	// Match must appear in a failure message, so a different failure is not mistaken for this one.
	Match string `json:"match"`
}

// T is the part of testing.T a check may use.
type T interface {
	Helper()
	Errorf(format string, args ...any)
	FailNow()
}

// Reporter is the part of testing.T that receives the outcome of a check.
type Reporter interface {
	T
	Logf(format string, args ...any)
}

// List maps check ids to their expected-failure entries.
type List map[string]Entry

// Load reads a JSON array of entries, each with an id, a reason and a tracking note.
func Load(path string) (List, error) {
	data, err := os.ReadFile(path) //nolint:gosec
	if err != nil {
		return nil, err
	}
	var entries []Entry
	if err = json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("xfail: parse %s: %w", path, err)
	}
	l := make(List, len(entries))
	for _, e := range entries {
		if e.ID == "" || e.Reason == "" || e.Tracking == "" || e.Match == "" {
			return nil, fmt.Errorf("xfail: entry %q needs an id, a reason, a tracking note and a match", e.ID)
		}
		if _, ok := l[e.ID]; ok {
			return nil, fmt.Errorf("xfail: duplicate entry %q", e.ID)
		}
		l[e.ID] = e
	}
	return l, nil
}

// Run runs check and fails t when the check fails unexpectedly or passes while listed.
func (l List) Run(t Reporter, id string, check func(T)) {
	t.Helper()

	rec := capture(check)
	e, listed := l[id]
	switch {
	case listed && rec.failed && rec.mentions(e.Match):
		t.Logf("expected failure (%s): %s", e.Tracking, e.Reason)
	case listed && rec.failed:
		t.Errorf("%s fails without %q; it may fail for a new reason:", id, e.Match)
		rec.replay(t)
	case listed:
		t.Errorf("%s passes now; remove it from the expected failures (%s)", id, e.Tracking)
	case rec.failed:
		rec.replay(t)
	}
}

// Unknown returns the sorted listed ids that are not in known.
func (l List) Unknown(known []string) []string {
	var ids []string
	for id := range l {
		if !slices.Contains(known, id) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// recorder collects the failures of a check instead of reporting them.
type recorder struct {
	msgs    []string
	failed  bool
	stopped bool
}

func (r *recorder) Helper() {}

func (r *recorder) Errorf(format string, args ...any) {
	r.failed = true
	r.msgs = append(r.msgs, fmt.Sprintf(format, args...))
}

func (r *recorder) FailNow() {
	r.failed = true
	r.stopped = true
	runtime.Goexit()
}

func (r *recorder) mentions(s string) bool {
	for _, msg := range r.msgs {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

func (r *recorder) replay(t T) {
	t.Helper()

	for _, msg := range r.msgs {
		t.Errorf("%s", msg)
	}
	if r.stopped {
		t.FailNow()
	}
}

// capture runs check on its own goroutine so FailNow can stop it without stopping the caller.
func capture(check func(T)) *recorder {
	rec := &recorder{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if v := recover(); v != nil {
				rec.Errorf("panic: %v", v)
			}
		}()
		check(rec)
	}()
	<-done
	return rec
}
