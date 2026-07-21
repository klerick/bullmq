package bullmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestQueueSizeLimit(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-sizelimit", WithClient(client))
	defer q.Close()

	// Data that serialises beyond the limit is rejected before enqueueing.
	if _, err := q.Add(ctx, "big", map[string]any{"k": "a value well over ten bytes"}, &JobOptions{SizeLimit: 10}); err == nil {
		t.Error("Add should reject data exceeding SizeLimit")
	}
	if n, _ := q.GetWaitingCount(ctx); n != 0 {
		t.Errorf("waiting = %d, want 0 (oversized job not enqueued)", n)
	}

	// Data within the limit is accepted.
	if _, err := q.Add(ctx, "small", map[string]any{"k": 1}, &JobOptions{SizeLimit: 1000}); err != nil {
		t.Errorf("Add within SizeLimit failed: %v", err)
	}
}

// A job whose backoff type is not builtin defers to the registered strategy.
func TestWorkerCustomBackoff(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	var mu sync.Mutex
	strategyCalls := 0
	strategy := func(attemptsMade int, backoffType string, err error, job *Job) int64 {
		mu.Lock()
		strategyCalls++
		mu.Unlock()
		if backoffType != "myBackoff" {
			t.Errorf("strategy backoffType = %q, want myBackoff", backoffType)
		}
		return 0 // immediate retry
	}

	q, _ := NewQueue("go-custombackoff", WithClient(client))
	defer q.Close()
	_, _ = q.Add(ctx, "flaky", nil, &JobOptions{Attempts: 2, Backoff: &BackoffOptions{Type: "myBackoff"}})

	attempts := 0
	done := make(chan struct{}, 1)
	w, _ := NewWorker("go-custombackoff", func(ctx context.Context, j *Job) (any, error) {
		mu.Lock()
		attempts++
		n := attempts
		mu.Unlock()
		if n < 2 {
			return nil, errors.New("transient")
		}
		done <- struct{}{}
		return nil, nil
	}, WithClient(client), WithBackoffStrategy(strategy))
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("job did not complete via custom-backoff retry")
	}

	mu.Lock()
	defer mu.Unlock()
	if strategyCalls < 1 {
		t.Errorf("custom strategy called %d times, want >= 1", strategyCalls)
	}
}
