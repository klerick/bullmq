// Package otel implements the bullmq.Telemetry interface with OpenTelemetry.
//
// Trace context is propagated using the W3C Trace Context format serialised as
// JSON, exactly like Node's @taskforcesh/bullmq-otel (`propagation.inject` into a
// carrier, then JSON.stringify). A trace started when a job is added therefore
// continues when the job is processed — whether by Go or by another BullMQ runtime.
package otel

import (
	"context"
	"encoding/json"

	bullmq "github.com/klerick/bullmq/go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Telemetry is an OpenTelemetry-backed bullmq.Telemetry.
type Telemetry struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
}

// New returns a Telemetry using the named tracer and the W3C Trace Context
// propagator (matching Node's bullmq-otel wire format).
func New(name string) *Telemetry {
	return &Telemetry{
		tracer:     otel.Tracer(name),
		propagator: propagation.TraceContext{},
	}
}

// StartSpan implements bullmq.Telemetry.
func (t *Telemetry) StartSpan(ctx context.Context, name string, kind bullmq.SpanKind) (context.Context, bullmq.Span) {
	ctx, span := t.tracer.Start(ctx, name, trace.WithSpanKind(toOtelKind(kind)))
	return ctx, &spanAdapter{span: span}
}

// Inject serialises the trace context to `{"traceparent":"…","tracestate":"…"}`,
// the same shape Node's bullmq-otel stores in the job's `tm`.
func (t *Telemetry) Inject(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	t.propagator.Inject(ctx, carrier)
	if len(carrier) == 0 {
		return ""
	}
	b, err := json.Marshal(map[string]string(carrier))
	if err != nil {
		return ""
	}
	return string(b)
}

// Extract restores a trace context from the serialised form.
func (t *Telemetry) Extract(ctx context.Context, metadata string) context.Context {
	if metadata == "" {
		return ctx
	}
	var carrier map[string]string
	if json.Unmarshal([]byte(metadata), &carrier) != nil {
		return ctx
	}
	return t.propagator.Extract(ctx, propagation.MapCarrier(carrier))
}

func toOtelKind(k bullmq.SpanKind) trace.SpanKind {
	switch k {
	case bullmq.SpanKindProducer:
		return trace.SpanKindProducer
	case bullmq.SpanKindConsumer:
		return trace.SpanKindConsumer
	default:
		return trace.SpanKindInternal
	}
}

type spanAdapter struct{ span trace.Span }

func (s *spanAdapter) SetAttributes(attrs map[string]any) {
	kvs := make([]attribute.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		kvs = append(kvs, toAttr(k, v))
	}
	s.span.SetAttributes(kvs...)
}

func (s *spanAdapter) RecordError(err error) {
	s.span.RecordError(err)
	s.span.SetStatus(codes.Error, err.Error())
}

func (s *spanAdapter) End() { s.span.End() }

func toAttr(k string, v any) attribute.KeyValue {
	switch val := v.(type) {
	case string:
		return attribute.String(k, val)
	case bool:
		return attribute.Bool(k, val)
	case int:
		return attribute.Int(k, val)
	case int64:
		return attribute.Int64(k, val)
	case float64:
		return attribute.Float64(k, val)
	default:
		return attribute.String(k, "")
	}
}
