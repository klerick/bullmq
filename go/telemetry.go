package bullmq

import "context"

// SpanKind mirrors the OpenTelemetry span kinds BullMQ uses.
type SpanKind int

const (
	SpanKindInternal SpanKind = iota
	SpanKindProducer
	SpanKindConsumer
)

// Telemetry lets callers plug in distributed tracing (e.g. OpenTelemetry). It is
// deliberately dependency-free and idiomatic Go: trace context travels in
// context.Context, and Inject/Extract (de)serialise it to the string BullMQ stores
// on a job (the `tm` option), so a trace started when a job is added continues when
// it is processed — even in another process or another BullMQ port. An OpenTelemetry
// implementation lives in the optional bullmq/otel subpackage.
type Telemetry interface {
	// StartSpan starts a span and returns a context carrying it plus the span.
	StartSpan(ctx context.Context, name string, kind SpanKind) (context.Context, Span)
	// Inject serialises the trace context in ctx to a string (stored as the job's
	// `tm`). Node's BullMQOtel uses W3C traceparent JSON; matching it makes traces
	// span Go and Node.
	Inject(ctx context.Context) string
	// Extract restores a trace context from a serialised string into ctx.
	Extract(ctx context.Context, metadata string) context.Context
}

// Span is a minimal span abstraction.
type Span interface {
	SetAttributes(attrs map[string]any)
	RecordError(err error)
	End()
}

// telemetryHelper wraps an optional Telemetry with no-op fast paths, so callers
// never nil-check.
type telemetryHelper struct {
	t    Telemetry
	name string // queue name, used as the span-name prefix
}

// trace runs fn inside a span named "{queue}.{op}". With no telemetry configured it
// just runs fn (span is nil). A non-nil error is recorded on the span.
func (h telemetryHelper) trace(ctx context.Context, kind SpanKind, op string, fn func(context.Context, Span) error) error {
	if h.t == nil {
		return fn(ctx, nil)
	}
	ctx, span := h.t.StartSpan(ctx, h.name+"."+op, kind)
	defer span.End()
	err := fn(ctx, span)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

// start opens a span for callers that finish it explicitly (see spanHandle.finish).
func (h telemetryHelper) start(ctx context.Context, kind SpanKind, op string) (context.Context, spanHandle) {
	if h.t == nil {
		return ctx, spanHandle{}
	}
	ctx, span := h.t.StartSpan(ctx, h.name+"."+op, kind)
	return ctx, spanHandle{span: span}
}

func (h telemetryHelper) inject(ctx context.Context) string {
	if h.t == nil {
		return ""
	}
	return h.t.Inject(ctx)
}

func (h telemetryHelper) extract(ctx context.Context, metadata string) context.Context {
	if h.t == nil || metadata == "" {
		return ctx
	}
	return h.t.Extract(ctx, metadata)
}

type spanHandle struct{ span Span }

func (s spanHandle) setAttrs(attrs map[string]any) {
	if s.span != nil {
		s.span.SetAttributes(attrs)
	}
}

func (s spanHandle) finish(err error) {
	if s.span == nil {
		return
	}
	if err != nil {
		s.span.RecordError(err)
	}
	s.span.End()
}
