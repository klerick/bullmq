package bullmq

import (
	"context"
	"strings"
	"testing"
	"time"
)

// waitForEvent drains the channel until an event matches, or fails on timeout.
func waitForEvent(t *testing.T, events <-chan QueueEvent, timeout time.Duration, match func(QueueEvent) bool) QueueEvent {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case e, ok := <-events:
			if !ok {
				t.Fatal("events channel closed before a match")
			}
			if match(e) {
				return e
			}
		case <-deadline:
			t.Fatal("timed out waiting for event")
		}
	}
}

// A completed job emits a "completed" event on the stream, observable by a
// separate QueueEvents listener.
func TestQueueEventsCompleted(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, _ := NewQueue("go-events", WithClient(client))
	defer q.Close()
	qe, _ := NewQueueEvents("go-events", WithClient(client))
	defer qe.Close()

	events, _ := qe.Listen(ctx)
	time.Sleep(300 * time.Millisecond) // let the first XREAD establish its position

	job, err := q.Add(ctx, "task", nil, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	w, _ := NewWorker("go-events", func(ctx context.Context, j *Job) (any, error) {
		return "done", nil
	}, WithClient(client))
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	e := waitForEvent(t, events, 8*time.Second, func(e QueueEvent) bool {
		return e.Event == EventCompleted && e.JobID == job.ID
	})
	if e.ReturnValue() != `"done"` {
		t.Errorf("completed event returnvalue = %q, want %q", e.ReturnValue(), `"done"`)
	}
}

// A job added by Node's BullMQ emits an "added" event that a Go QueueEvents
// listener receives (cross-process).
func TestQueueEventsInteropNodeAdd(t *testing.T) {
	client := requireRedis(t)
	requireNodeInterop(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	qe, _ := NewQueueEvents("go-events-i", WithClient(client))
	defer qe.Close()
	events, _ := qe.Listen(ctx)
	time.Sleep(400 * time.Millisecond)

	jobID := strings.TrimSpace(runNode(t, "add.mjs", "bull", "go-events-i", "greet", `{"hi":true}`))
	if jobID == "" {
		t.Fatal("node add returned empty id")
	}

	waitForEvent(t, events, 8*time.Second, func(e QueueEvent) bool {
		return e.Event == EventAdded && e.JobID == jobID
	})
}
