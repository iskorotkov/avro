package ghwebhooks_test

import (
	"flag"
	"os"
	"testing"
	"time"

	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/datasets/ghwebhooks"
)

const sampleEvents = 329

var tgzPath = flag.String("update", "", "regenerate sample.jsonl.zst from this webhooks-examples tarball")

func TestSample(t *testing.T) {
	if *tgzPath != "" {
		writeSample(t, *tgzPath)
	}

	ds, err := ghwebhooks.Provider{}.Load("", 0)
	if err != nil {
		t.Fatal(err)
	}
	events, err := dataset.Typed[ghwebhooks.Event](ds)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != sampleEvents {
		t.Fatalf("sample has %d events, want %d", len(events), sampleEvents)
	}

	var withRepo, withSender, pushes int
	for _, e := range events {
		if e.Event == "" {
			t.Fatalf("event without a name: %+v", e)
		}
		withRepo += b2i(e.Repository != nil)
		withSender += b2i(e.Sender != nil)
		pushes += b2i(len(e.Commits) > 0)
	}
	if withRepo < 250 || withSender < 300 || pushes == 0 {
		t.Fatalf("repository %d, sender %d, pushes %d", withRepo, withSender, pushes)
	}
}

func TestLoadRows(t *testing.T) {
	ds, err := ghwebhooks.Provider{}.Load("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(ds.Records()); n != 10 {
		t.Fatalf("Load(rows=10) returned %d records", n)
	}
}

func TestParsePayloadTimestamps(t *testing.T) {
	payload := `{"repository":{"id":1,"name":"r","created_at":1432000000,"pushed_at":"2015-05-05T23:40:30-04:00"},
		"head_commit":{"id":"a","timestamp":"2015-05-05T19:40:15.123456-04:00"}}`

	e, err := ghwebhooks.ParsePayload("push", []byte(payload))
	if err != nil {
		t.Fatal(err)
	}

	if want := time.Unix(1432000000, 0).UTC(); !e.Repository.CreatedAt.Equal(want) {
		t.Errorf("created_at = %v, want %v", e.Repository.CreatedAt, want)
	}
	if want := time.Date(2015, 5, 6, 3, 40, 30, 0, time.UTC); !e.Repository.PushedAt.Equal(want) {
		t.Errorf("pushed_at = %v, want %v", e.Repository.PushedAt, want)
	}
	if want := time.Date(2015, 5, 5, 23, 40, 15, 123_000_000, time.UTC); !e.HeadCommit.Timestamp.Equal(want) {
		t.Errorf("timestamp = %v, want %v", e.HeadCommit.Timestamp, want)
	}
	if e.Event != "push" || e.Repository.UpdatedAt != nil {
		t.Errorf("unexpected event: %+v", e)
	}
}

func writeSample(t *testing.T, path string) {
	t.Helper()

	events, err := ghwebhooks.ReadPackage(path, "api.github.com")
	if err != nil {
		t.Fatal(err)
	}

	f, err := os.Create("sample.jsonl.zst")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := dataset.WriteJSONL(f, events); err != nil {
		t.Fatal(err)
	}
}

func b2i(b bool) int {
	if b {
		return 1
	}

	return 0
}
