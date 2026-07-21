package otel

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"

	bullmq "github.com/klerick/bullmq/go"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Inject serialises a W3C traceparent JSON (matching Node's bullmq-otel), and
// Extract restores the same trace — so a trace propagates across the job boundary
// and across runtimes.
func TestInjectExtractRoundTrip(t *testing.T) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	tel := New("test")

	ctx, span := tel.StartSpan(context.Background(), "producer", bullmq.SpanKindProducer)
	otelSpan := trace.SpanFromContext(ctx)
	wantTrace := otelSpan.SpanContext().TraceID().String()
	if wantTrace == "00000000000000000000000000000000" {
		t.Fatal("expected a valid trace id from the SDK provider")
	}

	tm := tel.Inject(ctx)
	span.End()

	// The serialised form is {"traceparent":"00-<32hex>-<16hex>-<2hex>", ...}.
	var carrier map[string]string
	if err := json.Unmarshal([]byte(tm), &carrier); err != nil {
		t.Fatalf("tm is not JSON: %v (%q)", err, tm)
	}
	tp := carrier["traceparent"]
	if !regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`).MatchString(tp) {
		t.Fatalf("traceparent %q is not W3C format", tp)
	}

	// Extract into a fresh context and confirm the trace id survived.
	restored := tel.Extract(context.Background(), tm)
	gotTrace := trace.SpanContextFromContext(restored).TraceID().String()
	if gotTrace != wantTrace {
		t.Errorf("round-trip trace id = %s, want %s", gotTrace, wantTrace)
	}
}

// Extracting an empty or malformed metadata is a no-op, not a crash.
func TestExtractEmptyIsNoop(t *testing.T) {
	tel := New("test")
	if tel.Extract(context.Background(), "") == nil {
		t.Error("Extract(\"\") returned nil context")
	}
	if tel.Extract(context.Background(), "not json") == nil {
		t.Error("Extract(malformed) returned nil context")
	}
}
