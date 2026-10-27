// Package earthquakes provides the USGS weekly earthquake feed, a real-life dataset with nullable numbers, timestamps and short arrays.
package earthquakes

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/iskorotkov/avro/bench/dataset"
)

// Name is the dataset name.
const Name = "earthquakes"

const fullFile = "earthquakes.json"

var (
	//go:embed schema.avsc
	schema string
	//go:embed sample.jsonl.zst
	sample []byte
)

// Quake is the canonical record: one GeoJSON feature of the feed.
type Quake struct {
	ID       string    `avro:"id"       json:"id"`
	Mag      float64   `avro:"mag"      json:"mag"`
	Place    string    `avro:"place"    json:"place"`
	Time     time.Time `avro:"time"     json:"time"`
	Updated  time.Time `avro:"updated"  json:"updated"`
	TZ       int32     `avro:"tz"       json:"tz"`
	URL      string    `avro:"url"      json:"url"`
	Detail   string    `avro:"detail"   json:"detail"`
	Felt     *int32    `avro:"felt"     json:"felt"`
	CDI      *float64  `avro:"cdi"      json:"cdi"`
	MMI      *float64  `avro:"mmi"      json:"mmi"`
	Alert    *string   `avro:"alert"    json:"alert"`
	Status   string    `avro:"status"   json:"status"`
	Tsunami  int32     `avro:"tsunami"  json:"tsunami"`
	Sig      int32     `avro:"sig"      json:"sig"`
	Net      string    `avro:"net"      json:"net"`
	Code     string    `avro:"code"     json:"code"`
	IDs      []string  `avro:"ids"      json:"ids"`
	Sources  []string  `avro:"sources"  json:"sources"`
	Types    []string  `avro:"types"    json:"types"`
	NST      *int32    `avro:"nst"      json:"nst"`
	Dmin     *float64  `avro:"dmin"     json:"dmin"`
	RMS      *float64  `avro:"rms"      json:"rms"`
	Gap      *float64  `avro:"gap"      json:"gap"`
	MagType  string    `avro:"mag_type" json:"mag_type"`
	Type     string    `avro:"type"     json:"type"`
	Title    string    `avro:"title"    json:"title"`
	Geometry Point     `avro:"geometry" json:"geometry"`
}

// Point is the GeoJSON geometry of a Quake; Coordinates are longitude, latitude and depth in km.
type Point struct {
	Type        string    `avro:"type"        json:"type"`
	Coordinates []float64 `avro:"coordinates" json:"coordinates"`
}

// Provider loads the earthquakes dataset.
type Provider struct{}

// Name implements dataset.Provider.
func (Provider) Name() string { return Name }

// Sources implements dataset.Provider.
func (Provider) Sources() []dataset.Source {
	return []dataset.Source{{
		URL:    "https://cdn.jsdelivr.net/npm/vega-datasets@2.11.0/data/" + fullFile,
		SHA256: "a42702a83ffbae679f95d1fa53e2cae0bae13b21e599a68cdd50a44fc52129f7",
		File:   fullFile,
	}}
}

// Load implements dataset.Provider.
func (Provider) Load(dir string, rows int) (dataset.Dataset, error) {
	records, err := load(dir, rows)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", Name, err)
	}

	return dataset.New(Name, schema, records), nil
}

func load(dir string, rows int) ([]Quake, error) {
	if dir == "" {
		quakes, err := dataset.DecodeJSONL[Quake](sample)
		return dataset.Head(quakes, rows), err
	}

	data, err := os.ReadFile(filepath.Clean(filepath.Join(dir, fullFile)))
	if err != nil {
		return nil, err
	}

	quakes, err := ParseFeatures(data)

	return dataset.Head(quakes, rows), err
}

type featureCollection struct {
	Features []feature `json:"features"`
}

type feature struct {
	ID         string     `json:"id"`
	Properties properties `json:"properties"`
	Geometry   Point      `json:"geometry"`
}

type properties struct {
	Mag     float64  `json:"mag"`
	Place   string   `json:"place"`
	Time    int64    `json:"time"`
	Updated int64    `json:"updated"`
	TZ      int32    `json:"tz"`
	URL     string   `json:"url"`
	Detail  string   `json:"detail"`
	Felt    *int32   `json:"felt"`
	CDI     *float64 `json:"cdi"`
	MMI     *float64 `json:"mmi"`
	Alert   *string  `json:"alert"`
	Status  string   `json:"status"`
	Tsunami int32    `json:"tsunami"`
	Sig     int32    `json:"sig"`
	Net     string   `json:"net"`
	Code    string   `json:"code"`
	IDs     string   `json:"ids"`
	Sources string   `json:"sources"`
	Types   string   `json:"types"`
	NST     *int32   `json:"nst"`
	Dmin    *float64 `json:"dmin"`
	RMS     *float64 `json:"rms"`
	Gap     *float64 `json:"gap"`
	MagType string   `json:"magType"`
	Type    string   `json:"type"`
	Title   string   `json:"title"`
}

// ParseFeatures converts a USGS GeoJSON FeatureCollection into records.
func ParseFeatures(data []byte) ([]Quake, error) {
	var fc featureCollection
	if err := json.Unmarshal(data, &fc); err != nil {
		return nil, fmt.Errorf("parse feature collection: %w", err)
	}

	quakes := make([]Quake, len(fc.Features))
	for i, f := range fc.Features {
		q, err := f.quake()
		if err != nil {
			return nil, fmt.Errorf("feature %d (%s): %w", i, f.ID, err)
		}
		quakes[i] = q
	}

	return quakes, nil
}

func (f feature) quake() (Quake, error) {
	p := f.Properties

	ids, err := splitList(p.IDs)
	if err != nil {
		return Quake{}, fmt.Errorf("ids: %w", err)
	}
	sources, err := splitList(p.Sources)
	if err != nil {
		return Quake{}, fmt.Errorf("sources: %w", err)
	}
	types, err := splitList(p.Types)
	if err != nil {
		return Quake{}, fmt.Errorf("types: %w", err)
	}

	return Quake{
		ID: f.ID, Mag: p.Mag, Place: p.Place,
		Time: time.UnixMilli(p.Time).UTC(), Updated: time.UnixMilli(p.Updated).UTC(),
		TZ: p.TZ, URL: p.URL, Detail: p.Detail,
		Felt: p.Felt, CDI: p.CDI, MMI: p.MMI, Alert: p.Alert,
		Status: p.Status, Tsunami: p.Tsunami, Sig: p.Sig, Net: p.Net, Code: p.Code,
		IDs: ids, Sources: sources, Types: types,
		NST: p.NST, Dmin: p.Dmin, RMS: p.RMS, Gap: p.Gap,
		MagType: p.MagType, Type: p.Type, Title: p.Title,
		Geometry: f.Geometry,
	}, nil
}

// splitList splits the feed's ",a,b," lists; the result is never empty.
func splitList(s string) ([]string, error) {
	inner, ok := strings.CutPrefix(s, ",")
	if ok {
		inner, ok = strings.CutSuffix(inner, ",")
	}
	if !ok || inner == "" {
		return nil, fmt.Errorf("malformed list %q", s)
	}

	return strings.Split(inner, ","), nil
}
