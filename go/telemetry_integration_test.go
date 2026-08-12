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

// The flow path must propagate trace context exactly like the plain add path:
// every job of the tree carries `tm`, taken from its own producer span nested
// under the addFlow span — so a consumer continues the trace instead of opening a
// new root one. Mirrors src/classes/flow-producer.ts (addFlow/addNode + metadata).
func TestTelemetryFlowPropagates(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	tel := &pathTelemetry{}

	fp, err := NewFlowProducer(WithClient(client), WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}
	defer fp.Close()

	tree, err := fp.Add(ctx, &FlowJob{
		QueueName: "tel-parent", Name: "parent",
		Children: []*FlowJob{{QueueName: "tel-child", Name: "child"}},
	})
	if err != nil {
		t.Fatalf("flow Add: %v", err)
	}

	parentQ, _ := NewQueue("tel-parent", WithClient(client), WithTelemetry(tel))
	defer parentQ.Close()
	childQ, _ := NewQueue("tel-child", WithClient(client), WithTelemetry(tel))
	defer childQ.Close()

	// Stored in Redis, not just set in memory: the consumer reads it from there.
	storedParent, err := parentQ.GetJob(ctx, tree.Job.ID)
	if err != nil || storedParent == nil {
		t.Fatalf("GetJob(parent): %v", err)
	}
	wantParent := "tel-parent.addFlow>tel-parent.addNode"
	if got := toStr(storedParent.opts["tm"]); got != wantParent {
		t.Errorf("parent tm = %q, want %q", got, wantParent)
	}
	storedChild, err := childQ.GetJob(ctx, tree.Children[0].Job.ID)
	if err != nil || storedChild == nil {
		t.Fatalf("GetJob(child): %v", err)
	}
	wantChild := wantParent + ">tel-child.addNode"
	if got := toStr(storedChild.opts["tm"]); got != wantChild {
		t.Errorf("child tm = %q, want %q", got, wantChild)
	}

	if !tel.has("tel-parent.addFlow", SpanKindProducer) {
		t.Errorf("no addFlow producer span, got %v", tel.spans)
	}

	// End to end: the worker processing the child continues that very trace.
	done := make(chan struct{}, 1)
	w, err := NewWorker("tel-child", func(ctx context.Context, j *Job) (any, error) {
		done <- struct{}{}
		return nil, nil
	}, WithClient(client), WithTelemetry(tel))
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
	eventually(t, 3*time.Second, func() bool { return tel.sawExtracted(wantChild) })
}

// AddBulk opens a producer span but used to drop the metadata, so bulk-added jobs
// started a fresh trace on the worker. Upstream queue.ts addBulk propagates it.
func TestTelemetryAddBulkPropagates(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	tel := &pathTelemetry{}

	q, err := NewQueue("tel-bulk", WithClient(client), WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	jobs, err := q.AddBulk(ctx, []BulkJob{{Name: "a"}, {Name: "b"}})
	if err != nil {
		t.Fatalf("AddBulk: %v", err)
	}
	for _, j := range jobs {
		stored, err := q.GetJob(ctx, j.ID)
		if err != nil || stored == nil {
			t.Fatalf("GetJob(%s): %v", j.ID, err)
		}
		if got := toStr(stored.opts["tm"]); got != "tel-bulk.addBulk" {
			t.Errorf("job %s tm = %q, want %q", j.ID, got, "tel-bulk.addBulk")
		}
	}
}

// A scheduler's produced job must carry the trace context too (job-scheduler.ts
// puts the propagation metadata in the *job* opts, not in the stored template).
func TestTelemetrySchedulerPropagates(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	tel := &pathTelemetry{}

	q, err := NewQueue("tel-sched", WithClient(client), WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	job, err := q.UpsertJobScheduler(ctx, "sched-1", RepeatOptions{Every: 60000}, "tick", nil, nil)
	if err != nil {
		t.Fatalf("UpsertJobScheduler: %v", err)
	}
	stored, err := q.GetJob(ctx, job.ID)
	if err != nil || stored == nil {
		t.Fatalf("GetJob(%s): %v", job.ID, err)
	}
	if got := toStr(stored.opts["tm"]); got != "tel-sched.upsertJobScheduler" {
		t.Errorf("scheduled job tm = %q, want %q", got, "tel-sched.upsertJobScheduler")
	}
}
