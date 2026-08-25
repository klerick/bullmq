package otel

import (
	"context"
	"errors"
	"testing"
	"time"

	bullmq "github.com/klerick/bullmq/go"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// The acceptance test for the event-listener extension: a handler reacting to a
// queue event resolves the failed job's trace context and opens its span inside
// the producer's trace, instead of starting a fresh root. This is the last step of
// a flow whose parent was killed by the failParentOnFailure cascade — that parent
// never reaches a processor, so the event handler is where the outcome is recorded.
//
// With the accessor returning nothing the listener span lands in a different trace
// and this test fails, which is what makes it worth having.
func TestEventListenerJoinsProducerTrace(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	tel := New("bullmq-test")

	rootCtx, rootSpan := provider.Tracer("caller").Start(ctx, "http.request")
	wantTrace := trace.SpanContextFromContext(rootCtx).TraceID().String()

	q, err := bullmq.NewQueue("otel-ev", bullmq.WithClient(client), bullmq.WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	qe, err := bullmq.NewQueueEvents("otel-ev", bullmq.WithClient(client))
	if err != nil {
		t.Fatalf("NewQueueEvents: %v", err)
	}
	defer qe.Close()
	events, _ := qe.Listen(ctx)
	time.Sleep(200 * time.Millisecond) // let XREAD block before the job is added

	if _, err := q.Add(rootCtx, "task", nil, &bullmq.JobOptions{Attempts: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	rootSpan.End()

	w, err := bullmq.NewWorker("otel-ev", func(ctx context.Context, j *bullmq.Job) (any, error) {
		return nil, errors.New("boom")
	}, bullmq.WithClient(client), bullmq.WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	// The consumer side: react to the event, resolve the job's trace context, and
	// record the handler's work inside it.
	var handled bool
	for !handled {
		select {
		case ev := <-events:
			if ev.Event != bullmq.EventFailed {
				continue
			}
			tm, err := q.GetJobTelemetryMetadata(ctx, ev.JobID)
			if err != nil {
				t.Fatalf("GetJobTelemetryMetadata: %v", err)
			}
			evCtx := tel.Extract(ctx, tm)
			_, span := tel.StartSpan(evCtx, "listener.failed", bullmq.SpanKindConsumer)
			span.End()
			handled = true
		case <-ctx.Done():
			t.Fatal("failed event never arrived")
		}
	}

	var got string
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, s := range exporter.GetSpans() {
			if s.Name == "listener.failed" {
				got = s.SpanContext.TraceID().String()
			}
		}
		if got != "" || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got == "" {
		t.Fatal("listener span was never recorded")
	}
	if got != wantTrace {
		t.Errorf("listener span is in trace %s, want the producer's trace %s", got, wantTrace)
	}
}
