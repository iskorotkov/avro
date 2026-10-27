package wikirc_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/iskorotkov/avro/bench/datasets/wikirc"
)

func TestGenerateDeterministic(t *testing.T) {
	if diff := cmp.Diff(wikirc.Generate(50), wikirc.Generate(50)); diff != "" {
		t.Fatal(diff)
	}
}

func TestGenerateShape(t *testing.T) {
	events := wikirc.Generate(3000)

	counts := map[string]int{}
	wikis := map[string]bool{}
	bots := 0
	for i := range events {
		e := &events[i]
		counts[e.Type]++
		wikis[e.Wiki] = true
		if e.Bot {
			bots++
		}
		if e.Meta.DT.Location().String() != "UTC" || e.Meta.DT.Unix() != e.Timestamp {
			t.Fatalf("record %d: dt %v does not match timestamp %d", i, e.Meta.DT, e.Timestamp)
		}
		checkType(t, i, e)
	}

	if len(wikis) < 10 {
		t.Errorf("only %d wikis", len(wikis))
	}
	if share := float64(bots) / float64(len(events)); share < 0.2 || share > 0.4 {
		t.Errorf("bot share %.2f", share)
	}
	if share := float64(counts["edit"]) / float64(len(events)); share < 0.6 {
		t.Errorf("edit share %.2f", share)
	}
	for _, typ := range []string{"edit", "new", "log", "categorize"} {
		if counts[typ] == 0 {
			t.Errorf("no %s events", typ)
		}
	}
}

func checkType(t *testing.T, i int, e *wikirc.RecentChange) {
	t.Helper()

	isLog := e.Type == "log"
	if isLog != (e.LogID != nil && e.LogType != nil && e.LogAction != nil) {
		t.Fatalf("record %d: log fields do not match type %s", i, e.Type)
	}
	if !isLog && (e.LogParams != nil || e.LogActionComment != nil) {
		t.Fatalf("record %d: non-log event has log fields", i)
	}
	hasRev := e.Type == "edit" || e.Type == "new"
	if hasRev != (e.Revision != nil && e.Length != nil) {
		t.Fatalf("record %d: revision/length do not match type %s", i, e.Type)
	}
	if p := e.LogParams; p != nil {
		set := 0
		for _, b := range []bool{p.List != nil, p.Fields != nil, p.Text != nil} {
			if b {
				set++
			}
		}
		if set != 1 {
			t.Fatalf("record %d: %d log_params branches set", i, set)
		}
	}
}
