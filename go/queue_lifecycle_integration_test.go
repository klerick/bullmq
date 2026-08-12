package bullmq

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// Pausing no longer drains wait into a separate paused list (BullMQ v6 dropped
// that list): jobs stay in wait and the marker is deleted, which is what stops
// workers from picking them up. So the assertion is behavioural — a paused queue
// hands out nothing, a resumed one does.
func TestQueuePauseResume(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-pause", WithClient(client))
	defer q.Close()

	_, _ = q.Add(ctx, "task", nil, nil)
	if paused, _ := q.IsPaused(ctx); paused {
		t.Error("fresh queue should not be paused")
	}

	if err := q.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if paused, _ := q.IsPaused(ctx); !paused {
		t.Error("queue should be paused")
	}
	if n, _ := q.GetWaitingCount(ctx); n != 1 {
		t.Errorf("waiting while paused = %d, want 1 (v6 keeps jobs in wait)", n)
	}
	if n, _ := client.Exists(ctx, q.keys.Marker()).Result(); n != 0 {
		t.Error("marker should be dropped while paused, or workers keep waking up")
	}

	var processed atomic.Int64
	w, err := NewWorker("go-pause", func(ctx context.Context, j *Job) (any, error) {
		processed.Add(1)
		return nil, nil
	}, WithClient(client))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	time.Sleep(500 * time.Millisecond)
	if n := processed.Load(); n != 0 {
		t.Errorf("worker processed %d jobs while paused, want 0", n)
	}

	if err := q.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if paused, _ := q.IsPaused(ctx); paused {
		t.Error("queue should be resumed")
	}
	eventually(t, 5*time.Second, func() bool { return processed.Load() == 1 })
}

// A queue paused by a v5 runtime still holds jobs in the legacy paused list. The
// v6 script migrates them back to wait in batches of 7000 and reports how many
// are left, so Resume must keep calling it until none remain — one call would
// strand everything past the first batch.
func TestResumeMigratesLegacyPausedJobs(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-legacy-pause", WithClient(client))
	defer q.Close()

	// A job already in wait forces the batching path: with an empty wait the
	// script just renames the whole list in one go and never loops.
	if err := client.LPush(ctx, q.keys.Wait(), "existing").Err(); err != nil {
		t.Fatalf("seed wait: %v", err)
	}
	// Two batches worth, as a v5 runtime would have left them.
	const legacyJobs = 7003
	ids := make([]any, legacyJobs)
	for i := range ids {
		ids[i] = strconv.Itoa(i + 1)
	}
	if err := client.LPush(ctx, q.keys.Paused(), ids...).Err(); err != nil {
		t.Fatalf("seed legacy paused list: %v", err)
	}
	if err := client.HSet(ctx, q.keys.Meta(), "paused", 1).Err(); err != nil {
		t.Fatalf("mark paused: %v", err)
	}

	if err := q.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if n, _ := client.LLen(ctx, q.keys.Paused()).Result(); n != 0 {
		t.Errorf("%d jobs left in the legacy paused list, want 0", n)
	}
	if n, _ := client.LLen(ctx, q.keys.Wait()).Result(); n != legacyJobs+1 {
		t.Errorf("wait holds %d jobs, want all %d migrated plus the pre-existing one", n, legacyJobs)
	}
}

func TestQueueDrain(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-drain", WithClient(client))
	defer q.Close()

	_, _ = q.Add(ctx, "a", nil, nil)
	_, _ = q.Add(ctx, "b", nil, nil)
	_, _ = q.Add(ctx, "d", nil, &JobOptions{Delay: 60000})

	if err := q.Drain(ctx, false); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if n, _ := q.GetWaitingCount(ctx); n != 0 {
		t.Errorf("waiting after drain = %d, want 0", n)
	}
	if n, _ := q.GetDelayedCount(ctx); n != 1 {
		t.Errorf("delayed after drain(false) = %d, want 1 (kept)", n)
	}

	if err := q.Drain(ctx, true); err != nil {
		t.Fatalf("Drain(delayed): %v", err)
	}
	if n, _ := q.GetDelayedCount(ctx); n != 0 {
		t.Errorf("delayed after drain(true) = %d, want 0", n)
	}
}

func TestQueueRemove(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-remove", WithClient(client))
	defer q.Close()

	job, _ := q.Add(ctx, "task", nil, nil)
	removed, err := q.Remove(ctx, job.ID, true)
	if err != nil || !removed {
		t.Fatalf("Remove = %v, %v; want true", removed, err)
	}
	if got, _ := q.GetJob(ctx, job.ID); got != nil {
		t.Error("job still present after Remove")
	}
}

func TestQueueAddBulk(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-bulk", WithClient(client))
	defer q.Close()

	jobs, err := q.AddBulk(ctx, []BulkJob{
		{Name: "a", Data: map[string]any{"i": 1}},
		{Name: "b", Data: map[string]any{"i": 2}},
		{Name: "c", Data: map[string]any{"i": 3}},
	})
	if err != nil {
		t.Fatalf("AddBulk: %v", err)
	}
	if len(jobs) != 3 {
		t.Fatalf("AddBulk returned %d jobs, want 3", len(jobs))
	}
	for i, j := range jobs {
		if j.ID == "" {
			t.Errorf("job %d has no id", i)
		}
	}
	if n, _ := q.GetWaitingCount(ctx); n != 3 {
		t.Errorf("waiting after AddBulk = %d, want 3", n)
	}
}

func TestQueueObliterate(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-oblit", WithClient(client))
	defer q.Close()

	_, _ = q.Add(ctx, "a", nil, nil)
	_, _ = q.Add(ctx, "b", nil, nil)

	if err := q.Obliterate(ctx, true); err != nil {
		t.Fatalf("Obliterate: %v", err)
	}
	keys, err := client.Keys(ctx, "bull:go-oblit:*").Result()
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("queue keys remain after Obliterate: %v", keys)
	}
}

func TestQueueRetryJobs(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-retryjobs", WithClient(client))
	defer q.Close()

	_, _ = q.Add(ctx, "doomed", nil, &JobOptions{Attempts: 1})

	ran := make(chan struct{}, 1)
	w, _ := NewWorker("go-retryjobs", func(ctx context.Context, j *Job) (any, error) {
		select {
		case ran <- struct{}{}:
		default:
		}
		return nil, errors.New("boom")
	}, WithClient(client))
	go func() { _ = w.Run(ctx) }()
	<-ran
	eventually(t, 3*time.Second, func() bool { n, _ := q.GetFailedCount(ctx); return n == 1 })
	_ = w.Close() // stop processing so retried job stays in wait

	if err := q.RetryJobs(ctx, "failed", 100); err != nil {
		t.Fatalf("RetryJobs: %v", err)
	}
	if n, _ := q.GetFailedCount(ctx); n != 0 {
		t.Errorf("failed after RetryJobs = %d, want 0", n)
	}
	if n, _ := q.GetWaitingCount(ctx); n != 1 {
		t.Errorf("waiting after RetryJobs = %d, want 1", n)
	}
}
