package otel

import (
	"context"
	"os"
	"testing"
	"time"

	bullmq "github.com/klerick/bullmq/go"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// requireRedis skips when no server is reachable, keeping `go test` green on
// machines without Redis (same contract as the root package's integration tests).
func requireRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Skipf("redis not available (%v); skipping integration test", err)
	}
	return client
}

// The acceptance test for flow tracing with a real OpenTelemetry SDK: a whole flow
// tree added inside a caller's span, and the worker processing a child, must all
// land in the caller's trace. Without the propagation metadata on flow jobs the
// worker starts a brand new root trace instead — the symptom this fixes.
func TestFlowKeepsOneTraceWithRealTracer(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	tel := New("bullmq-test")

	// The caller's span — in production this is the incoming request.
	rootCtx, rootSpan := provider.Tracer("caller").Start(ctx, "http.request")
	wantTrace := trace.SpanContextFromContext(rootCtx).TraceID().String()

	fp, err := bullmq.NewFlowProducer(bullmq.WithClient(client), bullmq.WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}
	defer fp.Close()

	if _, err := fp.Add(rootCtx, &bullmq.FlowJob{
		QueueName: "otel-parent", Name: "parent",
		Children: []*bullmq.FlowJob{{QueueName: "otel-child", Name: "child"}},
	}); err != nil {
		t.Fatalf("flow Add: %v", err)
	}
	rootSpan.End()

	done := make(chan struct{}, 1)
	w, err := bullmq.NewWorker("otel-child", func(ctx context.Context, j *bullmq.Job) (any, error) {
		done <- struct{}{}
		return nil, nil
	}, bullmq.WithClient(client), bullmq.WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("child job was never processed")
	}

	// The consumer span is exported only after the job finishes; give it a moment.
	want := []string{"otel-parent.addFlow", "otel-parent.addNode", "otel-child.addNode", "otel-child.process"}
	deadline := time.Now().Add(5 * time.Second)
	var traces map[string]string
	for {
		traces = map[string]string{}
		for _, s := range exporter.GetSpans() {
			traces[s.Name] = s.SpanContext.TraceID().String()
		}
		if _, ok := traces["otel-child.process"]; ok || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	for _, name := range want {
		got, ok := traces[name]
		if !ok {
			t.Errorf("span %q was never recorded (got %v)", name, traces)
			continue
		}
		if got != wantTrace {
			t.Errorf("span %q is in trace %s, want the caller's trace %s", name, got, wantTrace)
		}
	}
}
