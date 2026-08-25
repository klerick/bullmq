package bullmq

import (
	"context"
	"testing"
	"time"
)

// The point of the whole feature: a process reacting to a queue EVENT can pick up
// the trace the job carries. The event stream has no trace context of its own (no
// Lua script writes `tm` into it), so the listener resolves it from the job.
func TestEventListenerCanResolveJobTrace(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	tel := &pathTelemetry{}
	q, err := NewQueue("ev-tel", WithClient(client), WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	qe, err := NewQueueEvents("ev-tel", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueueEvents: %v", err)
	}
	defer qe.Close()
	events, _ := qe.Listen(ctx)
	time.Sleep(200 * time.Millisecond) // let XREAD block before the job is added

	job, err := q.Add(ctx, "task", nil, &JobOptions{Attempts: 1})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	w, err := NewWorker("ev-tel", failingProcessor, WithClient(client), WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	ev := waitForEvent(t, events, 10*time.Second, func(e QueueEvent) bool { return e.Event == EventFailed })
	if ev.JobID != job.ID {
		t.Fatalf("failed event for job %q, want %q", ev.JobID, job.ID)
	}

	// The cheap path a listener uses: one HGET of the opts field, no full job read.
	tm, err := q.GetJobTelemetryMetadata(ctx, ev.JobID)
	if err != nil {
		t.Fatalf("GetJobTelemetryMetadata: %v", err)
	}
	if tm == "" {
		t.Fatal("no trace metadata for a job added with telemetry")
	}
	if tm != job.TelemetryMetadata() {
		t.Errorf("resolved tm = %q, want what the producer injected (%q)", tm, job.TelemetryMetadata())
	}

	// ...and the same value the full read yields, so the two paths cannot drift.
	stored, err := q.GetJob(ctx, ev.JobID)
	if err != nil || stored == nil {
		t.Fatalf("GetJob: %v (job %v)", err, stored)
	}
	if got := stored.TelemetryMetadata(); got != tm {
		t.Errorf("GetJob tm = %q, GetJobTelemetryMetadata = %q, want equal", got, tm)
	}
}

// The known limit, pinned so it stays a documented fallback rather than a crash:
// a job removed on failure takes its trace context with it, and the listener has
// to start a new trace.
func TestGetJobTelemetryMetadataOfRemovedJob(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	tel := &pathTelemetry{}
	q, err := NewQueue("ev-tel-gone", WithClient(client), WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	job, err := q.Add(ctx, "task", nil, &JobOptions{Attempts: 1, RemoveOnFail: true})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	w, err := NewWorker("ev-tel-gone", failingProcessor, WithClient(client), WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	eventually(t, 10*time.Second, func() bool {
		n, _ := client.Exists(ctx, q.keys.JobKey(job.ID)).Result()
		return n == 0
	})

	tm, err := q.GetJobTelemetryMetadata(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJobTelemetryMetadata of a removed job: %v, want no error", err)
	}
	if tm != "" {
		t.Errorf("tm = %q for a removed job, want empty", tm)
	}
}

// WithTelemetry on QueueEvents is accepted (the options are shared) but does
// nothing — upstream types it out entirely (QueueEventsOptions extends
// Omit<QueueBaseOptions, 'telemetry'>). This pins the contract: the listener
// never opens spans of its own, so a future half-implementation cannot sneak in.
func TestQueueEventsTelemetryOptionIsInert(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	listenerTel := &pathTelemetry{} // handed only to QueueEvents
	q, err := NewQueue("ev-tel-inert", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	qe, err := NewQueueEvents("ev-tel-inert", WithClient(client), WithTelemetry(listenerTel))
	if err != nil {
		t.Fatalf("NewQueueEvents: %v", err)
	}
	defer qe.Close()
	events, _ := qe.Listen(ctx)
	time.Sleep(200 * time.Millisecond)

	if _, err := q.Add(ctx, "task", nil, nil); err != nil {
		t.Fatalf("Add: %v", err)
	}
	waitForEvent(t, events, 10*time.Second, func(e QueueEvent) bool { return e.Event == EventAdded })

	listenerTel.mu.Lock()
	defer listenerTel.mu.Unlock()
	if len(listenerTel.spans) != 0 {
		t.Errorf("QueueEvents started spans %v, want none — the option is documented as ignored", listenerTel.spans)
	}
}
