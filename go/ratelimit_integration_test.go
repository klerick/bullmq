package bullmq

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestQueueGlobalControls(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-global", WithClient(client))
	defer q.Close()

	if err := q.SetGlobalConcurrency(ctx, 5); err != nil {
		t.Fatalf("SetGlobalConcurrency: %v", err)
	}
	if n, _ := q.GetGlobalConcurrency(ctx); n != 5 {
		t.Errorf("global concurrency = %d, want 5", n)
	}
	if err := q.RemoveGlobalConcurrency(ctx); err != nil {
		t.Fatalf("RemoveGlobalConcurrency: %v", err)
	}
	if n, _ := q.GetGlobalConcurrency(ctx); n != 0 {
		t.Errorf("global concurrency after remove = %d, want 0", n)
	}

	if err := q.SetGlobalRateLimit(ctx, 10, 1000); err != nil {
		t.Fatalf("SetGlobalRateLimit: %v", err)
	}
	max, dur, _ := q.GetGlobalRateLimit(ctx)
	if max != 10 || dur != 1000 {
		t.Errorf("global rate limit = (%d, %d), want (10, 1000)", max, dur)
	}
	_ = q.RemoveGlobalRateLimit(ctx)
	if max, dur, _ := q.GetGlobalRateLimit(ctx); max != 0 || dur != 0 {
		t.Errorf("global rate limit after remove = (%d, %d), want (0, 0)", max, dur)
	}
}

func TestQueueRateLimit(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-ratelimit", WithClient(client))
	defer q.Close()

	if err := q.RateLimit(ctx, 2000); err != nil {
		t.Fatalf("RateLimit: %v", err)
	}
	ttl, err := q.GetRateLimitTtl(ctx, 1)
	if err != nil {
		t.Fatalf("GetRateLimitTtl: %v", err)
	}
	if ttl <= 0 || ttl > 2000 {
		t.Errorf("rate limit ttl = %d, want in (0, 2000]", ttl)
	}

	if _, err := q.RemoveRateLimitKey(ctx); err != nil {
		t.Fatalf("RemoveRateLimitKey: %v", err)
	}
	if ttl, _ := q.GetRateLimitTtl(ctx, 1); ttl != 0 {
		t.Errorf("rate limit ttl after remove = %d, want 0", ttl)
	}
}

// A higher-priority job (lower number) is processed before a lower-priority one.
func TestWorkerPriorityOrder(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-prio-order", WithClient(client))
	defer q.Close()

	// Enqueue low priority first, then high priority.
	_, _ = q.Add(ctx, "low", nil, &JobOptions{Priority: 10})
	_, _ = q.Add(ctx, "high", nil, &JobOptions{Priority: 1})

	var mu sync.Mutex
	var order []string
	done := make(chan struct{}, 1)
	w, _ := NewWorker("go-prio-order", func(ctx context.Context, j *Job) (any, error) {
		mu.Lock()
		order = append(order, j.Name)
		n := len(order)
		mu.Unlock()
		if n == 2 {
			done <- struct{}{}
		}
		return "ok", nil
	}, WithClient(client))
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("jobs not processed; order = %v", order)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "high" {
		t.Errorf("processing order = %v, want high first", order)
	}
}
