package wikirc

import (
	"fmt"
	"html"
	"math/rand/v2"
	"strings"
	"time"
)

const (
	schemaID = "/mediawiki/recentchange/1.0.1"
	stream   = "mediawiki.recentchange"
	topic    = "eqiad.mediawiki.recentchange"
)

var start = time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)

type namespace struct {
	id     int32
	prefix string
	weight int
}

type logKind struct {
	typ     string
	actions []string
	weight  int
}

type wiki struct {
	db     string
	host   string
	weight int
	words  []string
	botPct int
}

var wikis = []wiki{
	{"enwiki", "en.wikipedia.org", 30, wordsEN, 20},
	{"wikidatawiki", "www.wikidata.org", 22, nil, 55},
	{"commonswiki", "commons.wikimedia.org", 12, wordsEN, 30},
	{"dewiki", "de.wikipedia.org", 6, wordsDE, 20},
	{"frwiki", "fr.wikipedia.org", 5, wordsFR, 20},
	{"eswiki", "es.wikipedia.org", 4, wordsES, 20},
	{"ruwiki", "ru.wikipedia.org", 5, wordsRU, 20},
	{"jawiki", "ja.wikipedia.org", 4, wordsJA, 20},
	{"zhwiki", "zh.wikipedia.org", 3, wordsZH, 20},
	{"arwiki", "ar.wikipedia.org", 2, wordsAR, 20},
	{"itwiki", "it.wikipedia.org", 3, wordsEN, 20},
	{"metawiki", "meta.wikimedia.org", 2, wordsEN, 20},
	{"enwiktionary", "en.wiktionary.org", 2, wordsEN, 25},
}

var (
	wordsEN = strings.Fields("River Mountain Harbour Garden Bridge Theory Station Orchestra Festival Council Valley Engine Library Island Forest Castle Market Railway Journal Academy Network Comet Lantern Meadow Quarry Beacon")
	wordsDE = strings.Fields("Bürgermeister Königsberg Schloss Straße Müller Wärme Übung Gemeinde Fluss Brücke Wald Hafen Käse Größe Stadtteil")
	wordsFR = strings.Fields("Château Église Forêt Rivière Général Café Bibliothèque Île Marché Théâtre Cœur Pâtisserie Hôtel Société")
	wordsES = strings.Fields("Niño Montaña Año Corazón Ciudad Estación Río Canción Jardín Pequeño Bosque Mercado Región")
	wordsRU = strings.Fields("Река Гора Город Театр Станция Лес Мост Площадь Музей Деревня Озеро Школа Сад")
	wordsJA = strings.Fields("川 山 駅 城 公園 学校 橋 町 港 神社 島 森 市場")
	wordsZH = strings.Fields("河流 山脉 车站 城堡 公园 学校 桥梁 港口 寺庙 岛屿 森林 市场")
	wordsAR = strings.Fields("نهر جبل محطة قلعة حديقة مدرسة جسر ميناء جزيرة غابة سوق")

	namespaces = []namespace{
		{0, "", 62},
		{1, "Talk:", 8},
		{2, "User:", 5},
		{3, "User talk:", 8},
		{4, "Project:", 3},
		{6, "File:", 4},
		{10, "Template:", 4},
		{14, "Category:", 6},
	}

	comments = []string{
		"fix typo", "copyedit", "add reference", "update figures", "revert vandalism",
		"clarify wording", "add link", "remove duplicate text", "fix formatting", "expand section",
		"merge stub", "correct date", "rm unsourced claim", "add infobox", "tidy up",
	}
	sections = strings.Fields("History Geography Demographics Reception Legacy Notes Overview Background")
	adjs     = strings.Fields("Quiet Amber Rapid Silver Mellow Brisk Hollow Crimson Gentle Lunar")
	nouns    = strings.Fields("Fox Heron Lynx Otter Falcon Badger Willow Maple Comet Pebble")

	logKinds = []logKind{
		{"patrol", []string{"patrol"}, 30},
		{"delete", []string{"delete", "restore", "revision"}, 18},
		{"move", []string{"move", "move_redir"}, 10},
		{"newusers", []string{"create", "autocreate"}, 14},
		{"upload", []string{"upload", "overwrite"}, 8},
		{"block", []string{"block", "unblock", "reblock"}, 6},
		{"rights", []string{"rights"}, 4},
		{"protect", []string{"protect", "modify", "unprotect"}, 5},
		{"thanks", []string{"thank"}, 5},
	}
)

type gen struct {
	rng      *rand.Rand
	now      time.Time
	offset   int64
	rcid     int64
	logid    int64
	revision map[string]int64
	request  string
}

// Generate returns n deterministic pseudo-random recent-change events in stream order.
func Generate(n int) []RecentChange {
	g := &gen{
		rng:      rand.New(rand.NewPCG(7, 11)), //nolint:gosec // Deterministic test data.
		now:      start,
		rcid:     1_480_000_000,
		logid:    310_000_000,
		revision: map[string]int64{},
	}
	events := make([]RecentChange, n)
	for i := range events {
		events[i] = g.event()
	}

	return events
}

func (g *gen) event() RecentChange {
	g.now = g.now.Add(time.Duration(1+g.rng.IntN(400)) * time.Millisecond)
	g.offset++
	g.rcid++

	w := pick(g.rng, wikis, func(w wiki) int { return w.weight })
	typ := g.changeType()
	bot := g.rng.IntN(100) < w.botPct && typ != "log"
	ns, title := g.page(w, typ)
	base := "https://" + w.host
	titleURL := base + "/wiki/" + strings.ReplaceAll(title, " ", "_")
	user := g.user(bot)
	ts := g.now.Unix()
	rcid := g.rcid

	e := RecentChange{
		Schema: schemaID,
		Meta: Meta{
			URI:       titleURL,
			RequestID: g.requestID(),
			ID:        g.uuid(),
			DT:        g.now,
			Domain:    w.host,
			Stream:    stream,
			Topic:     topic,
			Partition: 0,
			Offset:    1_900_000_000 + g.offset,
		},
		ID:               &rcid,
		Type:             typ,
		Namespace:        ns,
		Title:            title,
		TitleURL:         titleURL,
		Timestamp:        ts,
		User:             user,
		Bot:              bot,
		ServerURL:        base,
		ServerName:       w.host,
		ServerScriptPath: "/w",
		Wiki:             w.db,
	}

	switch typ {
	case "log":
		g.fillLog(&e)
	case "categorize":
		g.fillCategorize(&e)
	default:
		g.fillEdit(&e)
	}

	return e
}

func (g *gen) changeType() string {
	switch r := g.rng.IntN(100); {
	case r < 72:
		return "edit"
	case r < 79:
		return "new"
	case r < 89:
		return "log"
	default:
		return "categorize"
	}
}

func (g *gen) page(w wiki, typ string) (int32, string) {
	if typ == "categorize" {
		return 14, "Category:" + g.name(w)
	}
	if w.db == "wikidatawiki" && g.rng.IntN(10) < 8 {
		return 0, fmt.Sprintf("Q%d", 1+g.rng.IntN(30_000_000))
	}
	ns := pick(g.rng, namespaces, func(n namespace) int { return n.weight })
	if w.db == "commonswiki" && g.rng.IntN(10) < 6 {
		return 6, fmt.Sprintf("File:Synthetic %s %d.jpg", g.name(w), g.rng.IntN(5000))
	}
	if ns.id == 0 {
		return ns.id, g.name(w)
	}
	if ns.id == 2 || ns.id == 3 {
		return ns.id, ns.prefix + g.user(false)
	}

	return ns.id, ns.prefix + g.name(w)
}

func (g *gen) name(w wiki) string {
	words := w.words
	if words == nil {
		words = wordsEN
	}
	parts := make([]string, 1+g.rng.IntN(3))
	for i := range parts {
		parts[i] = words[g.rng.IntN(len(words))]
	}
	sep := " "
	if w.db == "jawiki" || w.db == "zhwiki" {
		sep = ""
	}

	return strings.Join(parts, sep)
}

func (g *gen) user(bot bool) string {
	switch r := g.rng.IntN(100); {
	case bot:
		return adjs[g.rng.IntN(len(adjs))] + nouns[g.rng.IntN(len(nouns))] + "Bot"
	case r < 18:
		return fmt.Sprintf("192.0.2.%d", g.rng.IntN(256))
	case r < 26:
		return fmt.Sprintf("198.51.100.%d", g.rng.IntN(256))
	case r < 32:
		return fmt.Sprintf("2001:db8:%x:%x::%x", g.rng.IntN(16), g.rng.IntN(16), g.rng.IntN(4096))
	default:
		return fmt.Sprintf("%s%s%d", adjs[g.rng.IntN(len(adjs))], nouns[g.rng.IntN(len(nouns))], g.rng.IntN(150))
	}
}

// requestID is shared by consecutive events, as one request can emit several.
func (g *gen) requestID() string {
	if g.request == "" || g.rng.IntN(100) < 55 {
		g.request = g.uuid()
	}

	return g.request
}

func (g *gen) uuid() string {
	var b [16]byte
	for i := range b {
		b[i] = byte(g.rng.UintN(256))
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func (g *gen) summary() (string, string) {
	if g.rng.IntN(100) < 25 {
		return "", ""
	}
	text := comments[g.rng.IntN(len(comments))]
	if g.rng.IntN(100) < 30 {
		sec := sections[g.rng.IntN(len(sections))]
		return "/* " + sec + " */ " + text,
			`<span dir="auto"><span class="autocomment">` + sec + `: </span> ` + html.EscapeString(text) + `</span>`
	}

	return text, `<span dir="auto">` + html.EscapeString(text) + `</span>`
}

func (g *gen) fillEdit(e *RecentChange) {
	e.Comment, e.ParsedComment = g.summary()
	minor := g.rng.IntN(100) < 20 || e.Bot && g.rng.IntN(2) == 0
	e.Minor = &minor

	if g.revision[e.Wiki] == 0 {
		g.revision[e.Wiki] = 1_000_000_000 + int64(g.rng.IntN(200_000_000))
	}
	newRev := g.revision[e.Wiki] + 1 + int64(g.rng.IntN(400))
	g.revision[e.Wiki] = newRev
	newLen := int64(200 + g.rng.IntN(1<<(8+g.rng.IntN(8))))
	if e.Type == "new" {
		e.Length = &Length{New: &newLen}
		e.Revision = &Revision{New: &newRev}
		e.NotifyURL = fmt.Sprintf("%s/w/index.php?oldid=%d&rcid=%d", e.ServerURL, newRev, *e.ID)
		patrolled := e.Bot || g.rng.IntN(100) < 30
		e.Patrolled = &patrolled

		return
	}

	oldRev := newRev - 1 - int64(g.rng.IntN(600))
	oldLen := max(newLen+int64(g.rng.IntN(401))-200, 0)
	e.Length = &Length{Old: &oldLen, New: &newLen}
	e.Revision = &Revision{Old: &oldRev, New: &newRev}
	e.NotifyURL = fmt.Sprintf("%s/w/index.php?diff=%d&oldid=%d&rcid=%d", e.ServerURL, newRev, oldRev, *e.ID)
	if g.rng.IntN(100) < 35 {
		patrolled := e.Bot || g.rng.IntN(2) == 0
		e.Patrolled = &patrolled
	}
}

func (g *gen) fillCategorize(e *RecentChange) {
	member := g.name(wikis[0])
	e.Comment = fmt.Sprintf("[[:%s]] added to category", member)
	e.ParsedComment = fmt.Sprintf(`<a href="/wiki/%s" title="%s">%s</a> added to category`, strings.ReplaceAll(member, " ", "_"), member, member)
	e.NotifyURL = fmt.Sprintf("%s/w/index.php?title=%s&curid=%d&rcid=%d", e.ServerURL, strings.ReplaceAll(e.Title, " ", "_"), 1+g.rng.IntN(90_000_000), *e.ID)
	minor := false
	e.Minor = &minor
	if g.rng.IntN(3) == 0 {
		e.ID = nil
	}
}

func (g *gen) fillLog(e *RecentChange) {
	kind := pick(g.rng, logKinds, func(k logKind) int { return k.weight })
	action := kind.actions[g.rng.IntN(len(kind.actions))]
	g.logid++
	logID := g.logid
	e.LogID, e.LogType, e.LogAction = &logID, &kind.typ, &action
	if kind.typ == "newusers" {
		e.Namespace, e.Title = -1, "Special:Log/newusers"
	}

	e.Comment, e.ParsedComment = g.summary()
	text := fmt.Sprintf("%s %s %s", e.User, action, e.Title)
	e.LogActionComment = &text
	e.NotifyURL = fmt.Sprintf("%s/w/index.php?title=Special:Log&logid=%d", e.ServerURL, logID)
	e.LogParams = g.logParams(kind.typ, e.Title)
}

func (g *gen) logParams(typ, title string) *LogParams {
	switch typ {
	case "patrol":
		rev := fmt.Sprint(1 + g.rng.IntN(900_000_000))
		return &LogParams{Fields: &map[string]string{"curid": rev, "previd": "0", "auto": fmt.Sprint(g.rng.IntN(2))}}
	case "move":
		return &LogParams{Fields: &map[string]string{"target": title + " (old)", "noredir": fmt.Sprint(g.rng.IntN(2))}}
	case "block":
		return &LogParams{Fields: &map[string]string{"duration": []string{"24 hours", "1 week", "infinite"}[g.rng.IntN(3)], "flags": "nocreate"}}
	case "upload":
		return &LogParams{Fields: &map[string]string{"img_sha1": fmt.Sprintf("%040x", g.rng.Uint64()), "img_timestamp": g.now.Format("20060102150405")}}
	case "newusers":
		return &LogParams{Fields: &map[string]string{"userid": fmt.Sprint(1 + g.rng.IntN(80_000_000))}}
	case "rights":
		return &LogParams{List: &[]string{"autoconfirmed", "extendedconfirmed"}}
	case "protect":
		return &LogParams{List: &[]string{"edit=sysop", "move=sysop"}}
	case "delete":
		if g.rng.IntN(2) == 0 {
			return &LogParams{List: &[]string{fmt.Sprint(g.rng.IntN(20)), "0"}}
		}
	case "thanks":
		text := "legacy"
		return &LogParams{Text: &text}
	}

	return nil
}

func pick[T any](rng *rand.Rand, items []T, weight func(T) int) T {
	total := 0
	for _, it := range items {
		total += weight(it)
	}
	r := rng.IntN(total)
	for _, it := range items {
		if r -= weight(it); r < 0 {
			return it
		}
	}

	return items[len(items)-1]
}
