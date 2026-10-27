package otel

import (
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"time"
)

const (
	schemaURL    = "https://opentelemetry.io/schemas/1.26.0"
	kindInternal = "SPAN_KIND_INTERNAL"
	kindServer   = "SPAN_KIND_SERVER"
	kindClient   = "SPAN_KIND_CLIENT"
	kindProducer = "SPAN_KIND_PRODUCER"
	kindConsumer = "SPAN_KIND_CONSUMER"
	statusUnset  = "STATUS_CODE_UNSET"
	statusOK     = "STATUS_CODE_OK"
	statusError  = "STATUS_CODE_ERROR"
)

// Generate returns n deterministic pseudo-random span batches.
func Generate(n int) []ResourceSpans {
	g := &generator{
		rng:  rand.New(rand.NewPCG(7, 11)), //nolint:gosec // Deterministic test data.
		next: time.Date(2026, 3, 2, 9, 30, 0, 0, time.UTC),
	}
	g.makePods()
	records := make([]ResourceSpans, n)
	for i := range records {
		records[i] = g.batch()
	}

	return records
}

type attrs = map[string]AnyValue

type pod struct {
	svc      *service
	resource attrs
}

type service struct {
	name, lang, version string
	routes              []string
	scopes              []scopeKind
}

type scopeKind struct {
	name, version string
	build         func(g *generator, b *batch) Span
}

// anchor is an entry span that later spans of the batch hang under.
type anchor struct {
	trace      TraceID
	id         SpanID
	start, end time.Time
}

type batch struct {
	pod     *pod
	base    time.Time
	anchors []anchor
}

type generator struct {
	rng  *rand.Rand
	next time.Time
	pods []*pod
}

func (g *generator) services() []*service {
	web := []scopeKind{
		{"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp", "0.53.0", (*generator).httpServer},
		{"app/handlers", "1.0.0", (*generator).internal},
		{"gorm.io/plugin/opentelemetry", "0.1.4", (*generator).db},
		{"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp/client", "0.53.0", (*generator).httpClient},
	}
	worker := []scopeKind{
		{"github.com/segmentio/kafka-go/otelkafka", "0.9.2", (*generator).consumer},
		{"app/worker", "2.3.1", (*generator).internal},
		{"github.com/redis/go-redis/extra/redisotel", "9.5.1", (*generator).redis},
		{"github.com/segmentio/kafka-go/otelkafka/producer", "0.9.2", (*generator).producer},
	}
	rpc := []scopeKind{
		{"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc", "0.53.0", (*generator).grpcServer},
		{"app/service", "3.1.0", (*generator).internal},
		{"go.mongodb.org/mongo-driver/mongo/otelmongo", "0.2.1", (*generator).mongo},
	}

	return []*service{
		{"frontend", "nodejs", "4.12.0", []string{"GET /", "GET /product/{id}", "POST /cart", "GET /checkout"}, web},
		{"cart-service", "go", "1.14.3", []string{"GET /api/cart/{id}", "PUT /api/cart/{id}/items", "DELETE /api/cart/{id}"}, web},
		{"checkout-service", "go", "2.8.1", []string{"POST /api/checkout", "GET /api/checkout/{id}/status"}, web},
		{"catalog-service", "python", "0.31.7", []string{"GET /api/products", "GET /api/products/{sku}", "GET /api/search"}, web},
		{"payment-service", "java", "5.2.0", []string{"POST /v1/charges", "POST /v1/refunds", "GET /v1/charges/{id}"}, web},
		{"order-processor", "java", "3.9.4", nil, worker},
		{"email-worker", "python", "1.2.9", nil, worker},
		{"inventory-service", "go", "1.7.2", nil, rpc},
		{"shipping-service", "go", "2.0.5", nil, rpc},
	}
}

var (
	regions = []string{"eu-west-1", "us-east-1", "ap-southeast-2"}
	runtime = map[string][3]string{
		"go":     {"go", "go1.26.4", "gc"},
		"java":   {"OpenJDK Runtime Environment", "21.0.4+7-LTS", "Eclipse Adoptium"},
		"python": {"CPython", "3.12.5", "CPython"},
		"nodejs": {"nodejs", "22.9.0", "Node.js"},
	}
)

func (g *generator) makePods() {
	for _, svc := range g.services() {
		for range 3 {
			g.pods = append(g.pods, &pod{svc: svc, resource: g.resource(svc)})
		}
	}
}

func (g *generator) resource(svc *service) attrs {
	region := pick(g, regions)
	rt := runtime[svc.lang]
	node := fmt.Sprintf("ip-10-%d-%d-%d.%s.compute.internal", g.rng.IntN(4), g.rng.IntN(256), g.rng.IntN(256), region)
	podName := fmt.Sprintf("%s-%s-%s", svc.name, g.hex(5), g.hex(3))

	return build(
		kv("service.name", svc.name), kv("service.namespace", "shop"), kv("service.version", svc.version),
		kv("service.instance.id", g.uuid()),
		kv("telemetry.sdk.name", "opentelemetry"), kv("telemetry.sdk.language", svc.lang), kv("telemetry.sdk.version", "1.28.0"),
		kv("deployment.environment.name", "production"),
		kv("k8s.cluster.name", "prod-"+region), kv("k8s.namespace.name", "shop"), kv("k8s.pod.name", podName),
		kv("k8s.pod.uid", g.uuid()), kv("k8s.node.name", node), kv("k8s.container.name", svc.name),
		kv("container.id", g.hex(32)), kv("host.name", podName), kv("os.type", "linux"),
		kv("cloud.provider", "aws"), kv("cloud.region", region), kv("cloud.availability_zone", region+"a"),
		kv("process.pid", 1+g.rng.IntN(40)), kv("process.runtime.name", rt[0]), kv("process.runtime.version", rt[1]),
		kv("process.runtime.description", rt[2]),
	)
}

func (g *generator) batch() ResourceSpans {
	p := g.pods[g.rng.IntN(len(g.pods))]
	b := &batch{pod: p, base: g.next}
	g.next = g.next.Add(time.Duration(50+g.rng.IntN(900)) * time.Millisecond)

	rs := ResourceSpans{
		Resource:  Resource{Attributes: p.resource, DroppedAttributesCount: 0},
		SchemaURL: schemaURL,
	}
	for _, sk := range p.svc.scopes {
		if g.rng.IntN(4) == 0 {
			continue
		}
		spans := make([]Span, 1+g.rng.IntN(3))
		for i := range spans {
			spans[i] = sk.build(g, b)
		}
		rs.ScopeSpans = append(rs.ScopeSpans, ScopeSpans{
			Scope:     InstrumentationScope{Name: sk.name, Version: sk.version, Attributes: attrs{}},
			SchemaURL: schemaURL,
			Spans:     spans,
		})
	}
	if rs.ScopeSpans == nil {
		first := p.svc.scopes[0]
		rs.ScopeSpans = []ScopeSpans{{
			Scope:     InstrumentationScope{Name: first.name, Version: first.version, Attributes: attrs{}},
			SchemaURL: schemaURL,
			Spans:     []Span{first.build(g, b)},
		}}
	}

	return rs
}

// entry starts a span without a parent in the batch and registers it as an anchor.
func (g *generator) entry(b *batch, kind, name string, median time.Duration) Span {
	s := g.span(kind, name, b.base.Add(time.Duration(g.rng.Int64N(int64(2*time.Second)))), median)
	s.TraceID = g.traceID()
	if g.rng.IntN(10) < 3 {
		s.ParentSpanID = nil
	} else {
		up := g.spanID()
		s.ParentSpanID = &up
	}
	b.anchors = append(b.anchors, anchor{trace: s.TraceID, id: s.SpanID, start: s.StartTime, end: s.EndTime})

	return s
}

// child starts a span under a random anchor, or a fresh remote trace when the batch has none.
func (g *generator) child(b *batch, kind, name string, median time.Duration) Span {
	if len(b.anchors) == 0 {
		s := g.span(kind, name, b.base.Add(time.Duration(g.rng.Int64N(int64(2*time.Second)))), median)
		s.TraceID = g.traceID()
		up := g.spanID()
		s.ParentSpanID = &up

		return s
	}

	a := b.anchors[g.rng.IntN(len(b.anchors))]
	span := a.end.Sub(a.start)
	start := a.start.Add(time.Duration(float64(span) * (0.05 + 0.4*g.rng.Float64())))
	s := g.span(kind, name, start, median)
	if s.EndTime.After(a.end) {
		s.EndTime = a.end
	}
	s.TraceID = a.trace
	parent := a.id
	s.ParentSpanID = &parent

	return s
}

func (g *generator) span(kind, name string, start time.Time, median time.Duration) Span {
	// Rounding to microseconds hides last-ulp differences between math.Exp implementations.
	dur := time.Duration(math.Round(float64(median)*math.Exp(g.rng.NormFloat64()*0.8)/1e3)) * time.Microsecond
	s := Span{
		SpanID:     g.spanID(),
		Name:       name,
		Kind:       kind,
		StartTime:  start.Truncate(time.Microsecond),
		EndTime:    start.Add(dur + time.Duration(g.rng.IntN(1000))).Truncate(time.Microsecond),
		Attributes: attrs{},
		Events:     []Event{},
		Links:      []Link{},
		Status:     Status{Code: statusUnset},
	}
	if g.rng.IntN(20) == 0 {
		s.TraceState = "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE"
	}
	if g.rng.IntN(100) == 0 {
		s.DroppedAttributesCount = 1 + g.rng.Int32N(5)
	}

	return s
}

func (g *generator) event(s *Span, name string, at float64, a attrs) {
	t := s.StartTime.Add(time.Duration(float64(s.EndTime.Sub(s.StartTime)) * at)).Truncate(time.Microsecond)
	s.Events = append(s.Events, Event{Time: t, Name: name, Attributes: a})
}

func (g *generator) fail(s *Span, msg, errType, lang string) {
	s.Status = Status{Code: statusError, Message: msg}
	s.Attributes["error.type"] = str(errType)
	g.event(s, "exception", 0.9, build(
		kv("exception.type", errType), kv("exception.message", msg), kv("exception.stacktrace", g.stack(lang, errType, msg)),
	))
}

func (g *generator) httpServer(b *batch) Span {
	svc := b.pod.svc
	route := pick(g, svc.routes)
	method, path, _ := strings.Cut(route, " ")
	s := g.entry(b, kindServer, route, 35*time.Millisecond)
	code := g.httpStatus()
	s.Attributes = build(
		kv("http.request.method", method), kv("http.route", path), kv("url.path", strings.NewReplacer("{id}", fmt.Sprint(g.rng.IntN(90000)), "{sku}", g.sku()).Replace(path)),
		kv("url.scheme", "https"), kv("http.response.status_code", code), kv("network.protocol.name", "http"),
		kv("network.protocol.version", pick(g, []string{"1.1", "2"})), kv("server.address", svc.name+".shop.svc.cluster.local"),
		kv("server.port", 8080), kv("client.address", g.ip()), kv("user_agent.original", pick(g, userAgents)),
		kv("http.request.body.size", g.rng.IntN(4096)), kv("http.response.body.size", g.rng.IntN(65536)),
		kv("http.request.header.x-forwarded-for", g.ip(), g.ip()),
		kv("http.response.header.content-type", "application/json"),
	)
	if g.rng.IntN(3) == 0 {
		s.Attributes["url.query"] = str(fmt.Sprintf("currency=%s&page=%d", pick(g, []string{"EUR", "USD", "GBP"}), g.rng.IntN(20)))
	}
	if g.rng.IntN(20) == 0 {
		s.Attributes["http.request.body.digest"] = AnyValue{BytesValue: g.bytes(8)}
	}
	if g.rng.IntN(5) == 0 {
		s.Attributes["app.feature_flag.new_checkout"] = AnyValue{BoolValue: new(g.rng.IntN(2) == 0)}
	}
	switch {
	case code >= 500:
		g.fail(&s, fmt.Sprintf("HTTP %d", code), fmt.Sprint(code), svc.lang)
	case code < 400 && g.rng.IntN(4) == 0:
		s.Status.Code = statusOK
	}
	if g.rng.IntN(4) == 0 {
		g.event(&s, "log", 0.5, build(
			kv("log.severity", "INFO"), kv("log.message", "request authorised for tenant "+pick(g, tenants)),
		))
	}
	if g.rng.IntN(30) == 0 {
		s.Links = append(s.Links, g.link(build(kv("link.kind", "retry_of"))))
	}

	return s
}

func (g *generator) httpClient(b *batch) Span {
	host := pick(g, []string{"payments.shop.svc.cluster.local", "api.stripe.example.com", "geo.internal.example.com"})
	method := pick(g, []string{"GET", "POST", "PUT"})
	s := g.child(b, kindClient, method, 22*time.Millisecond)
	code := g.httpStatus()
	s.Attributes = build(
		kv("http.request.method", method), kv("url.full", fmt.Sprintf("https://%s/v1/%s/%d", host, pick(g, []string{"charges", "quotes", "lookup"}), g.rng.IntN(99999))),
		kv("server.address", host), kv("server.port", 443), kv("http.response.status_code", code),
		kv("network.protocol.version", "1.1"), kv("http.request.resend_count", g.rng.IntN(3)),
	)
	if code >= 500 {
		g.fail(&s, pick(g, []string{"context deadline exceeded", "connection reset by peer", "upstream returned 503"}), "timeout", b.pod.svc.lang)
	}

	return s
}

func (g *generator) db(b *batch) Span {
	table := pick(g, []string{"orders", "carts", "products", "customers", "payments"})
	op := pick(g, []string{"SELECT", "INSERT", "UPDATE"})
	s := g.child(b, kindClient, op+" "+table, 3*time.Millisecond)
	s.Attributes = build(
		kv("db.system", "postgresql"), kv("db.namespace", "shop"), kv("db.operation.name", op), kv("db.collection.name", table),
		kv("db.query.text", fmt.Sprintf("%s * FROM %s WHERE id = $1 AND tenant = $2", op, table)),
		kv("server.address", "pg-primary.shop.svc.cluster.local"), kv("server.port", 5432), kv("db.response.returned_rows", g.rng.IntN(50)),
		kv("app.cart.total", math.Round(g.rng.Float64()*50000)/100),
	)
	if g.rng.IntN(40) == 0 {
		g.fail(&s, "canceling statement due to statement timeout", "57014", b.pod.svc.lang)
	}

	return s
}

func (g *generator) redis(b *batch) Span {
	cmd := pick(g, []string{"GET", "SET", "HGETALL", "EXPIRE"})
	s := g.child(b, kindClient, cmd, 600*time.Microsecond)
	s.Attributes = build(
		kv("db.system", "redis"), kv("db.operation.name", cmd), kv("db.namespace", "0"),
		kv("db.query.text", cmd+" session:"+g.hex(8)), kv("server.address", "redis.shop.svc.cluster.local"), kv("server.port", 6379),
	)

	return s
}

func (g *generator) mongo(b *batch) Span {
	coll := pick(g, []string{"inventory", "shipments", "reservations"})
	op := pick(g, []string{"find", "insert", "update", "aggregate"})
	s := g.child(b, kindClient, op+" "+coll, 4*time.Millisecond)
	s.Attributes = build(
		kv("db.system", "mongodb"), kv("db.namespace", "shop"), kv("db.operation.name", op), kv("db.collection.name", coll),
		kv("server.address", "mongo-0.mongo.shop.svc.cluster.local"), kv("server.port", 27017),
	)

	return s
}

func (g *generator) grpcServer(b *batch) Span {
	method := pick(g, []string{"Reserve", "Release", "GetStock", "Quote", "Ship"})
	svc := b.pod.svc.name
	s := g.entry(b, kindServer, "shop."+svc+"/"+method, 8*time.Millisecond)
	status := pick(g, []int{0, 0, 0, 0, 0, 0, 0, 5, 14})
	s.Attributes = build(
		kv("rpc.system", "grpc"), kv("rpc.service", "shop."+svc), kv("rpc.method", method),
		kv("rpc.grpc.status_code", status), kv("server.address", svc+".shop.svc.cluster.local"), kv("server.port", 9090),
		kv("network.peer.address", g.ip()), kv("network.peer.port", 32768+g.rng.IntN(28000)),
	)
	if status != 0 {
		g.fail(&s, fmt.Sprintf("rpc error: code = %d", status), "grpc.Status", b.pod.svc.lang)
	}

	return s
}

func (g *generator) producer(b *batch) Span {
	topic := pick(g, topics)
	s := g.child(b, kindProducer, topic+" publish", 2*time.Millisecond)
	s.Attributes = build(
		kv("messaging.system", "kafka"), kv("messaging.destination.name", topic), kv("messaging.operation.type", "send"),
		kv("messaging.kafka.partition", g.rng.IntN(12)), kv("messaging.kafka.offset", g.rng.Int64N(5_000_000)),
		kv("messaging.message.id", g.uuid()), kv("messaging.message.body.size", 200+g.rng.IntN(4000)),
		kv("messaging.client.id", b.pod.svc.name+"@"+g.hex(4)),
	)
	g.event(&s, "message.sent", 0.8, build(kv("message.id", g.rng.Int64N(1_000_000))))

	return s
}

func (g *generator) consumer(b *batch) Span {
	topic := pick(g, topics)
	s := g.entry(b, kindConsumer, topic+" process", 18*time.Millisecond)
	s.Attributes = build(
		kv("messaging.system", "kafka"), kv("messaging.destination.name", topic), kv("messaging.operation.type", "process"),
		kv("messaging.consumer.group.name", b.pod.svc.name), kv("messaging.kafka.partition", g.rng.IntN(12)),
		kv("messaging.kafka.offset", g.rng.Int64N(5_000_000)), kv("messaging.message.body.size", 200+g.rng.IntN(4000)),
		kv("messaging.batch.message_count", 1+g.rng.IntN(5)),
	)
	for range 1 + g.rng.IntN(3) {
		s.Links = append(s.Links, g.link(build(kv("messaging.operation.type", "publish"))))
	}
	if g.rng.IntN(25) == 0 {
		g.fail(&s, "failed to deserialise message", "SerializationError", b.pod.svc.lang)
	}

	return s
}

func (g *generator) internal(b *batch) Span {
	fn := pick(g, []string{"validateCart", "applyDiscounts", "renderTemplate", "computeTotals", "loadTenantConfig"})
	s := g.child(b, kindInternal, fn, 900*time.Microsecond)
	items := 1 + g.rng.IntN(5)
	ids := make([]AnyValue, items)
	for i := range ids {
		ids[i] = AnyValue{IntValue: new(g.rng.Int64N(1_000_000))}
	}
	s.Attributes = build(
		kv("code.function.name", "app."+fn), kv("code.filepath", "internal/"+strings.ToLower(fn)+".go"), kv("code.lineno", 10+g.rng.IntN(400)),
		kv("app.cart.items", items), kv("app.tenant", pick(g, tenants)),
	)
	s.Attributes["app.cart.item_ids"] = AnyValue{ArrayValue: ids}
	g.event(&s, fn+".done", 0.95, build(kv("app.result", pick(g, []string{"ok", "cached", "skipped"})), kv("app.items", items)))

	return s
}

func (g *generator) link(a attrs) Link {
	return Link{TraceID: g.traceID(), SpanID: g.spanID(), Attributes: a}
}

func (g *generator) httpStatus() int {
	r := g.rng.IntN(100)
	switch {
	case r < 90:
		return pick(g, []int{200, 200, 200, 201, 204, 304})
	case r < 96:
		return pick(g, []int{400, 401, 404, 429})
	default:
		return pick(g, []int{500, 502, 503})
	}
}

var (
	userAgents = []string{
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 Version/17.5 Mobile/15E148 Safari/604.1",
		"okhttp/4.12.0",
		"kube-probe/1.30",
	}
	tenants = []string{"acme", "globex", "initech", "umbrella", "hooli"}
	topics  = []string{"orders.created", "orders.paid", "emails.outbound", "inventory.adjusted"}
)

var frames = map[string][]string{
	"go": {
		"github.com/shop/%s/internal/store.(*Repo).Save(repo.go:%d)",
		"github.com/shop/%s/internal/service.(*Service).Handle(service.go:%d)",
		"net/http.HandlerFunc.ServeHTTP(server.go:%d)",
		"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp.(*middleware).serveHTTP(handler.go:%d)",
	},
	"java": {
		"com.shop.%s.repo.ChargeRepository.save(ChargeRepository.java:%d)",
		"com.shop.%s.service.ChargeService.charge(ChargeService.java:%d)",
		"org.springframework.web.servlet.FrameworkServlet.service(FrameworkServlet.java:%d)",
		"org.apache.tomcat.util.threads.TaskThread$WrappingRunnable.run(TaskThread.java:%d)",
	},
	"python": {
		"File \"/app/%s/views.py\", line %d, in handle",
		"File \"/app/%s/db.py\", line %d, in fetch",
		"File \"/usr/local/lib/python3.12/site-packages/flask/app.py\", line %d, in dispatch_request",
	},
	"nodejs": {
		"at Object.handle (/app/%s/handler.js:%d:15)",
		"at Layer.handle [as handle_request] (/app/node_modules/express/lib/router/layer.js:%d:5)",
		"at next (/app/node_modules/express/lib/router/route.js:%d:13)",
	},
}

func (g *generator) stack(lang, errType, msg string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s: %s", errType, msg)
	fs := frames[lang]
	for range 4 + g.rng.IntN(6) {
		sb.WriteString("\n\t")
		fmt.Fprintf(&sb, pick(g, fs), pick(g, []string{"cart", "payment", "orders", "catalog"}), 20+g.rng.IntN(900))
	}

	return sb.String()
}

func pick[T any](g *generator, from []T) T { return from[g.rng.IntN(len(from))] }

func (g *generator) hex(n int) string {
	const digits = "0123456789abcdef"
	b := make([]byte, n*2)
	for i := range b {
		b[i] = digits[g.rng.IntN(len(digits))]
	}

	return string(b)
}

func (g *generator) uuid() string {
	h := g.hex(16)

	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func (g *generator) sku() string { return fmt.Sprintf("SKU-%05d", g.rng.IntN(100000)) }

func (g *generator) ip() string {
	nets := [...]string{"192.0.2", "198.51.100", "203.0.113"}
	return fmt.Sprintf("%s.%d", nets[g.rng.IntN(len(nets))], 1+g.rng.IntN(254))
}

func (g *generator) bytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(g.rng.UintN(256))
	}

	return b
}

func (g *generator) traceID() TraceID {
	var id TraceID
	copy(id[:], g.bytes(len(id)))

	return id
}

func (g *generator) spanID() SpanID {
	var id SpanID
	copy(id[:], g.bytes(len(id)))

	return id
}

type pair struct {
	key string
	val AnyValue
}

// kv builds an attribute; several strings become an array value.
func kv(key string, v any, more ...string) pair {
	if len(more) > 0 {
		vals := []AnyValue{str(v.(string))}
		for _, m := range more {
			vals = append(vals, str(m))
		}

		return pair{key, AnyValue{ArrayValue: vals}}
	}
	switch v := v.(type) {
	case string:
		return pair{key, str(v)}
	case int:
		return pair{key, AnyValue{IntValue: new(int64(v))}}
	case int64:
		return pair{key, AnyValue{IntValue: new(v)}}
	case float64:
		return pair{key, AnyValue{DoubleValue: new(v)}}
	default:
		panic(fmt.Sprintf("unsupported attribute type %T", v))
	}
}

func build(pairs ...pair) attrs {
	m := make(attrs, len(pairs))
	for _, p := range pairs {
		m[p.key] = p.val
	}

	return m
}

func str(s string) AnyValue { return AnyValue{StringValue: &s} }
