package bullmq

import (
	"context"
	"testing"
	"time"
)

func TestCalculateBackoffJitter(t *testing.T) {
	b := map[string]any{"type": "fixed", "delay": int64(1000), "jitter": 0.5}
	for i := 0; i < 50; i++ {
		d := calculateBackoff(b, 1)
		if d < 500 || d > 1000 {
			t.Fatalf("jittered delay %d out of [500, 1000]", d)
		}
	}
}

func TestQueueGetWorkers(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-workers", WithClient(client))
	defer q.Close()

	w, _ := NewWorker("go-workers", func(ctx context.Context, j *Job) (any, error) {
		return nil, nil
	}, WithClient(client), WithWorkerName("worker-1"))
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	eventually(t, 6*time.Second, func() bool {
		n, _ := q.GetWorkersCount(ctx)
		return n >= 1
	})
}

// Two jobs with the same deduplication id collapse into one.
func TestQueueDeduplication(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-dedup", WithClient(client))
	defer q.Close()

	j1, err := q.Add(ctx, "task", nil, &JobOptions{Deduplication: &DeduplicationOptions{ID: "dup1"}})
	if err != nil {
		t.Fatalf("Add 1: %v", err)
	}
	j2, err := q.Add(ctx, "task", nil, &JobOptions{Deduplication: &DeduplicationOptions{ID: "dup1"}})
	if err != nil {
		t.Fatalf("Add 2: %v", err)
	}
	if j1.ID != j2.ID {
		t.Errorf("dedup: j1=%s j2=%s, want the same id", j1.ID, j2.ID)
	}
	if n, _ := q.GetWaitingCount(ctx); n != 1 {
		t.Errorf("waiting = %d, want 1 (second add deduplicated)", n)
	}

	if id, _ := q.GetDeduplicationJobID(ctx, "dup1"); id != j1.ID {
		t.Errorf("GetDeduplicationJobID = %q, want %q", id, j1.ID)
	}
	if _, err := q.RemoveDeduplicationKey(ctx, "dup1"); err != nil {
		t.Fatalf("RemoveDeduplicationKey: %v", err)
	}
	if id, _ := q.GetDeduplicationJobID(ctx, "dup1"); id != "" {
		t.Errorf("GetDeduplicationJobID after remove = %q, want empty", id)
	}
}
