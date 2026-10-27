package protobuf

import "github.com/iskorotkov/avro/bench/datasets/earthquakes"

func fromQuake(q *earthquakes.Quake) *Quake {
	return &Quake{
		Id:       q.ID,
		Mag:      q.Mag,
		Place:    q.Place,
		Time:     q.Time.UnixMilli(),
		Updated:  q.Updated.UnixMilli(),
		Tz:       q.TZ,
		Url:      q.URL,
		Detail:   q.Detail,
		Felt:     q.Felt,
		Cdi:      q.CDI,
		Mmi:      q.MMI,
		Alert:    q.Alert,
		Status:   q.Status,
		Tsunami:  q.Tsunami,
		Sig:      q.Sig,
		Net:      q.Net,
		Code:     q.Code,
		Ids:      q.IDs,
		Sources:  q.Sources,
		Types:    q.Types,
		Nst:      q.NST,
		Dmin:     q.Dmin,
		Rms:      q.RMS,
		Gap:      q.Gap,
		MagType:  q.MagType,
		Type:     q.Type,
		Title:    q.Title,
		Geometry: &QuakePoint{Type: q.Geometry.Type, Coordinates: q.Geometry.Coordinates},
	}
}
