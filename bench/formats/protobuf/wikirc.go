package protobuf

import (
	"strings"

	"github.com/iskorotkov/avro/bench/datasets/wikirc"
)

func fromRecentChange(e *wikirc.RecentChange) *RcChange {
	return &RcChange{
		Schema: e.Schema,
		Meta: &RcMeta{
			Uri:       e.Meta.URI,
			RequestId: e.Meta.RequestID,
			Id:        e.Meta.ID,
			Dt:        e.Meta.DT.UnixMilli(),
			Domain:    e.Meta.Domain,
			Stream:    e.Meta.Stream,
			Topic:     e.Meta.Topic,
			Partition: e.Meta.Partition,
			Offset:    e.Meta.Offset,
		},
		Id:               e.ID,
		Type:             enum[RcChangeType](RcChangeType_value, "RC_CHANGE_TYPE_"+strings.ToUpper(e.Type)),
		Namespace:        e.Namespace,
		Title:            e.Title,
		TitleUrl:         e.TitleURL,
		Comment:          e.Comment,
		Parsedcomment:    e.ParsedComment,
		Timestamp:        e.Timestamp,
		User:             e.User,
		Bot:              e.Bot,
		Minor:            e.Minor,
		Patrolled:        e.Patrolled,
		Length:           fromRcLength(e.Length),
		Revision:         fromRcRevision(e.Revision),
		ServerUrl:        e.ServerURL,
		ServerName:       e.ServerName,
		ServerScriptPath: e.ServerScriptPath,
		Wiki:             e.Wiki,
		NotifyUrl:        e.NotifyURL,
		LogId:            e.LogID,
		LogType:          e.LogType,
		LogAction:        e.LogAction,
		LogActionComment: e.LogActionComment,
		LogParams:        fromRcLogParams(e.LogParams),
	}
}

func fromRcLength(l *wikirc.Length) *RcLength {
	if l == nil {
		return nil
	}

	return &RcLength{Old: l.Old, New: l.New}
}

func fromRcRevision(r *wikirc.Revision) *RcRevision {
	if r == nil {
		return nil
	}

	return &RcRevision{Old: r.Old, New: r.New}
}

func fromRcLogParams(p *wikirc.LogParams) *RcLogParams {
	switch {
	case p == nil:
		return nil
	case p.List != nil:
		return &RcLogParams{Params: &RcLogParams_List{List: &RcStringList{Items: *p.List}}}
	case p.Fields != nil:
		return &RcLogParams{Params: &RcLogParams_Fields{Fields: &RcStringMap{Values: *p.Fields}}}
	case p.Text != nil:
		return &RcLogParams{Params: &RcLogParams_Text{Text: *p.Text}}
	default:
		return &RcLogParams{}
	}
}
