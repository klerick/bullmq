package bullmq

import (
	"context"
	"sync"
	"testing"
	"time"
)

type mockSpanRec struct {
	name string
	kind SpanKind
}

type mockTelemetry struct {
	mu        sync.Mutex
	spans     []mockSpanRec
	injected  int
	extracted string
}

func (m *mockTelemetry) StartSpan(ctx context.Context, name string, kind SpanKind) (context.Context, Span) {
	m.mu.Lock()
	m.spans = append(m.spans, mockSpanRec{name, kind})
	m.mu.Unlock()
	return ctx, mockSpan{}
}

func (m *mockTelemetry) Inject(ctx context.Context) string {
	m.mu.Lock()
	m.injected++
	m.mu.Unlock()
	return "TRACE-CTX"
}

func (m *mockTelemetry) Extract(ctx context.Context, metadata string) context.Context {
	m.mu.Lock()
	m.extracted = metadata
	m.mu.Unlock()
	return ctx
}

type mockSpan struct{}

func (mockSpan) SetAttributes(map[string]any) {}
func (mockSpan) RecordError(error)            {}
func (mockSpan) End()                         {}

func (m *mockTelemetry) has(name string, kind SpanKind) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.spans {
		if s.name == name && s.kind == kind {
			return true
		}
	}
	return false
}

// Add creates a producer span and injects trace context; processing extracts it
// and creates a consumer span — the context travels through the job's `tm`.
func TestTelemetryAddAndProcess(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	mock := &mockTelemetry{}

	q, _ := NewQueue("go-tel", WithClient(client), WithTelemetry(mock))
	defer q.Close()
	if _, err := q.Add(ctx, "task", nil, nil); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if !mock.has("go-tel.add", SpanKindProducer) {
		t.Error("no producer span for add")
	}
	if mock.injected == 0 {
		t.Error("Add did not inject trace context")
	}

	done := make(chan struct{}, 1)
	w, _ := NewWorker("go-tel", func(ctx context.Context, j *Job) (any, error) {
		done <- struct{}{}
		return nil, nil
	}, WithClient(client), WithTelemetry(mock))
	defer w.Close()
	go func() { _ = w.Run(ctx) }()
	<-done

	// The consumer span appears, and the tm injected at add-time is what the worker
	// extracts — proving the trace context propagated through the job.
	eventually(t, 3*time.Second, func() bool { return mock.has("go-tel.process", SpanKindConsumer) })
	mock.mu.Lock()
	extracted := mock.extracted
	mock.mu.Unlock()
	if extracted != "TRACE-CTX" {
		t.Errorf("worker extracted %q, want the injected \"TRACE-CTX\"", extracted)
	}
}
