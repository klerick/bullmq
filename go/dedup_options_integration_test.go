package bullmq

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// heldWorker runs jobs that block until the test releases them, so a job can be
// kept in the active state while more adds arrive.
type heldWorker struct {
	started chan string // job id, as each job starts
	release chan struct{}
}

func newHeldWorker(t *testing.T, ctx context.Context, client *redis.Client, queue string) *heldWorker {
	t.Helper()
	h := &heldWorker{started: make(chan string, 8), release: make(chan struct{})}
	w, err := NewWorker(queue, func(ctx context.Context, j *Job) (any, error) {
		h.started <- j.ID
		select {
		case <-h.release:
		case <-ctx.Done():
		}
		return "done", nil
	}, WithClient(client))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	go func() { _ = w.Run(ctx) }()
	return h
}

// The holder of the key is still waiting: a second add collapses into it, which is
// correct — that job has not read anything yet, so it will cover the new change
// too. This is today's behaviour and must not move.
func TestDeduplicationCollapsesAgainstWaitingJob(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("dedup-waiting", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	first, err := q.Add(ctx, "calc", map[string]any{"v": "1"},
		&JobOptions{Deduplication: &DeduplicationOptions{ID: "k", KeepLastIfActive: true}})
	if err != nil {
		t.Fatalf("first Add: %v", err)
	}
	second, err := q.Add(ctx, "calc", map[string]any{"v": "2"},
		&JobOptions{Deduplication: &DeduplicationOptions{ID: "k", KeepLastIfActive: true}})
	if err != nil {
		t.Fatalf("second Add: %v", err)
	}
	if second.ID != first.ID {
		t.Errorf("second Add id = %q, want the waiting job's id %q", second.ID, first.ID)
	}
	if n, _ := q.GetWaitingCount(ctx); n != 1 {
		t.Errorf("waiting = %d, want 1 — the second add must not create a job", n)
	}
}

// The holder is active, so it may already have read its input: with
// KeepLastIfActive the new add is not dropped but parked, and promoted into a real
// job once the active one finishes. Without it the update would be lost for good.
func TestDeduplicationKeepsLastWhileActive(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("dedup-active", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	opts := func(v string) *JobOptions {
		return &JobOptions{Deduplication: &DeduplicationOptions{ID: "k", KeepLastIfActive: true}}
	}
	worker := newHeldWorker(t, ctx, client, "dedup-active")

	first, err := q.Add(ctx, "calc", map[string]any{"v": "1"}, opts("1"))
	if err != nil {
		t.Fatalf("first Add: %v", err)
	}
	var activeID string
	select {
	case activeID = <-worker.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the first job never started")
	}
	if activeID != first.ID {
		t.Fatalf("worker started %q, want %q", activeID, first.ID)
	}

	// Two more updates arrive mid-flight; only the last one should survive.
	if _, err := q.Add(ctx, "calc", map[string]any{"v": "2"}, opts("2")); err != nil {
		t.Fatalf("Add while active: %v", err)
	}
	if _, err := q.Add(ctx, "calc", map[string]any{"v": "3"}, opts("3")); err != nil {
		t.Fatalf("Add while active: %v", err)
	}
	if n, _ := q.GetWaitingCount(ctx); n != 0 {
		t.Errorf("waiting = %d while the holder is active, want 0 — the adds are parked, not queued", n)
	}

	close(worker.release)

	// Exactly one job is promoted, carrying the newest payload.
	var promoted *Job
	eventually(t, 10*time.Second, func() bool {
		jobs, err := q.GetJobs(ctx, []string{"waiting", "active", "completed"}, 0, -1, false)
		if err != nil {
			return false
		}
		for _, j := range jobs {
			if j.ID != first.ID {
				promoted = j
				return true
			}
		}
		return false
	})
	data, _ := promoted.Data.(map[string]any)
	if data["v"] != "3" {
		t.Errorf("promoted job data = %v, want the last add's payload (v=3)", promoted.Data)
	}

	counts, err := q.GetJobCounts(ctx, "waiting", "active", "completed")
	if err != nil {
		t.Fatalf("GetJobCounts: %v", err)
	}
	if total := counts["waiting"] + counts["active"] + counts["completed"]; total != 2 {
		t.Errorf("jobs = %v (total %d), want exactly 2: the original and one promotion", counts, total)
	}
}

// The default is unchanged: against an active holder, an add without
// KeepLastIfActive is dropped. Pinned so the new option cannot quietly become the
// default.
func TestDeduplicationDropsAddAgainstActiveByDefault(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("dedup-default", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	worker := newHeldWorker(t, ctx, client, "dedup-default")
	first, err := q.Add(ctx, "calc", map[string]any{"v": "1"},
		&JobOptions{Deduplication: &DeduplicationOptions{ID: "k"}})
	if err != nil {
		t.Fatalf("first Add: %v", err)
	}
	select {
	case <-worker.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the first job never started")
	}

	if _, err := q.Add(ctx, "calc", map[string]any{"v": "2"},
		&JobOptions{Deduplication: &DeduplicationOptions{ID: "k"}}); err != nil {
		t.Fatalf("Add while active: %v", err)
	}
	close(worker.release)

	eventually(t, 10*time.Second, func() bool {
		n, _ := q.GetCompletedCount(ctx)
		return n == 1
	})
	time.Sleep(500 * time.Millisecond) // give a (non-existent) promotion time to appear
	counts, _ := q.GetJobCounts(ctx, "waiting", "active", "completed")
	if total := counts["waiting"] + counts["active"] + counts["completed"]; total != 1 {
		t.Errorf("jobs = %v (total %d), want 1 — without KeepLastIfActive the add is dropped", counts, total)
	}
	if n, _ := q.GetCompletedCount(ctx); n != 1 {
		t.Errorf("completed = %d, want the original job %q only", n, first.ID)
	}
}

// replace swaps the record of a job that is still delayed: the old one is removed
// and the new payload takes the key.
func TestDeduplicationReplaceOnDelayedJob(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("dedup-replace", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	opts := &JobOptions{
		Delay:         30000,
		Deduplication: &DeduplicationOptions{ID: "k", Replace: true},
	}
	first, err := q.Add(ctx, "calc", map[string]any{"v": "1"}, opts)
	if err != nil {
		t.Fatalf("first Add: %v", err)
	}
	second, err := q.Add(ctx, "calc", map[string]any{"v": "2"}, opts)
	if err != nil {
		t.Fatalf("second Add: %v", err)
	}
	if second.ID == first.ID {
		t.Fatalf("replace returned the old id %q, want a new job", second.ID)
	}

	if n, _ := q.GetDelayedCount(ctx); n != 1 {
		t.Errorf("delayed = %d, want 1 — the old record is replaced, not added to", n)
	}
	gone, err := q.GetJob(ctx, first.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if gone != nil {
		t.Errorf("the replaced job %q still exists", first.ID)
	}
	kept, err := q.GetJob(ctx, second.ID)
	if err != nil || kept == nil {
		t.Fatalf("the replacing job is missing: %v", err)
	}
	if data, _ := kept.Data.(map[string]any); data["v"] != "2" {
		t.Errorf("kept job data = %v, want the new payload", kept.Data)
	}
}
