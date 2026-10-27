// Package wikirc provides the synthetic Wikimedia recent-changes dataset with nullable records and a union-like record.
package wikirc

import (
	_ "embed"
	"fmt"
	"time"

	"github.com/iskorotkov/avro/bench/dataset"
)

// Name is the dataset name.
const Name = "wikirc"

var (
	//go:embed schema.avsc
	schema string
	//go:embed sample.jsonl.zst
	sample []byte
)

// RecentChange is the canonical record, modelled after the mediawiki/recentchange 1.0.1 event.
type RecentChange struct {
	Schema           string     `avro:"schema"             json:"$schema"`
	Meta             Meta       `avro:"meta"               json:"meta"`
	ID               *int64     `avro:"id"                 json:"id"`
	Type             string     `avro:"type"               json:"type"`
	Namespace        int32      `avro:"namespace"          json:"namespace"`
	Title            string     `avro:"title"              json:"title"`
	TitleURL         string     `avro:"title_url"          json:"title_url"`
	Comment          string     `avro:"comment"            json:"comment"`
	ParsedComment    string     `avro:"parsedcomment"      json:"parsedcomment"`
	Timestamp        int64      `avro:"timestamp"          json:"timestamp"`
	User             string     `avro:"user"               json:"user"`
	Bot              bool       `avro:"bot"                json:"bot"`
	Minor            *bool      `avro:"minor"              json:"minor"`
	Patrolled        *bool      `avro:"patrolled"          json:"patrolled"`
	Length           *Length    `avro:"length"             json:"length"`
	Revision         *Revision  `avro:"revision"           json:"revision"`
	ServerURL        string     `avro:"server_url"         json:"server_url"`
	ServerName       string     `avro:"server_name"        json:"server_name"`
	ServerScriptPath string     `avro:"server_script_path" json:"server_script_path"`
	Wiki             string     `avro:"wiki"               json:"wiki"`
	NotifyURL        string     `avro:"notify_url"         json:"notify_url"`
	LogID            *int64     `avro:"log_id"             json:"log_id"`
	LogType          *string    `avro:"log_type"           json:"log_type"`
	LogAction        *string    `avro:"log_action"         json:"log_action"`
	LogActionComment *string    `avro:"log_action_comment" json:"log_action_comment"`
	LogParams        *LogParams `avro:"log_params"         json:"log_params"`
}

// Meta is the event envelope.
type Meta struct {
	URI       string    `avro:"uri"        json:"uri"`
	RequestID string    `avro:"request_id" json:"request_id"`
	ID        string    `avro:"id"         json:"id"`
	DT        time.Time `avro:"dt"         json:"dt"`
	Domain    string    `avro:"domain"     json:"domain"`
	Stream    string    `avro:"stream"     json:"stream"`
	Topic     string    `avro:"topic"      json:"topic"`
	Partition int32     `avro:"partition"  json:"partition"`
	Offset    int64     `avro:"offset"     json:"offset"`
}

// Length is the page size in bytes before and after the change.
type Length struct {
	Old *int64 `avro:"old" json:"old"`
	New *int64 `avro:"new" json:"new"`
}

// Revision holds the revision IDs before and after the change.
type Revision struct {
	Old *int64 `avro:"old" json:"old"`
	New *int64 `avro:"new" json:"new"`
}

// LogParams models the array, object or string log_params of the source schema; at most one branch is set.
type LogParams struct {
	List   *[]string          `avro:"list"   json:"list"`
	Fields *map[string]string `avro:"fields" json:"fields"`
	Text   *string            `avro:"text"   json:"text"`
}

// Provider loads the wikirc dataset.
type Provider struct{}

// Name implements dataset.Provider.
func (Provider) Name() string { return Name }

// Sources implements dataset.Provider; the dataset is synthetic, so only the sample exists.
func (Provider) Sources() []dataset.Source { return nil }

// Load implements dataset.Provider.
func (Provider) Load(_ string, rows int) (dataset.Dataset, error) {
	records, err := dataset.DecodeJSONL[RecentChange](sample)
	if err != nil {
		return nil, fmt.Errorf("%s sample: %w", Name, err)
	}

	return dataset.New(Name, schema, dataset.Head(records, rows)), nil
}
