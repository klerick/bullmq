package bullmq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestQueuePauseResume(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
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
	if n, _ := q.GetWaitingCount(ctx); n != 0 {
		t.Errorf("waiting after pause = %d, want 0 (moved to paused)", n)
	}

	if err := q.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if paused, _ := q.IsPaused(ctx); paused {
		t.Error("queue should be resumed")
	}
	if n, _ := q.GetWaitingCount(ctx); n != 1 {
		t.Errorf("waiting after resume = %d, want 1", n)
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
