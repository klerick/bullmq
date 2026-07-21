package bullmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// eventually polls cond until it holds or the timeout elapses.
func eventually(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

// A worker fetches a queued job, runs the processor, and moves it to completed
// with the return value stored.
func TestWorkerProcessesJob(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("go-worker", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()
	job, err := q.Add(ctx, "task", map[string]any{"x": 1}, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	got := make(chan any, 1)
	w, err := NewWorker("go-worker", func(ctx context.Context, j *Job) (any, error) {
		got <- j.Data
		return map[string]any{"ok": true}, nil
	}, WithClient(client))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("job was not processed in time")
	}

	// The job should land in completed with its return value stored.
	eventually(t, 3*time.Second, func() bool {
		return client.ZScore(ctx, q.keys.Completed(), job.ID).Err() == nil
	})
	if rv := client.HGet(ctx, q.keys.JobKey(job.ID), "returnvalue").Val(); rv != `{"ok":true}` {
		t.Errorf("returnvalue = %q, want %q", rv, `{"ok":true}`)
	}
}

// A failing job with attempts and backoff is retried via the delayed set and
// eventually completes on a later attempt.
func TestWorkerRetriesWithBackoff(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("go-retry", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()
	job, err := q.Add(ctx, "flaky", nil, &JobOptions{
		Attempts: 3,
		Backoff:  &BackoffOptions{Type: "fixed", Delay: 200},
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	var mu sync.Mutex
	attempts := 0
	done := make(chan struct{}, 1)
	w, err := NewWorker("go-retry", func(ctx context.Context, j *Job) (any, error) {
		mu.Lock()
		attempts++
		n := attempts
		mu.Unlock()
		if n < 3 {
			return nil, errors.New("transient failure")
		}
		done <- struct{}{}
		return "recovered", nil
	}, WithClient(client))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		mu.Lock()
		n := attempts
		mu.Unlock()
		t.Fatalf("job did not complete after retries; attempts so far = %d", n)
	}

	eventually(t, 3*time.Second, func() bool {
		return client.ZScore(ctx, q.keys.Completed(), job.ID).Err() == nil
	})
	mu.Lock()
	defer mu.Unlock()
	if attempts != 3 {
		t.Errorf("processor ran %d times, want 3 (2 failures + 1 success)", attempts)
	}
}

// A job that exhausts its attempts lands in the failed set with its reason stored.
func TestWorkerMovesToFailed(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("go-fail", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()
	job, err := q.Add(ctx, "doomed", nil, &JobOptions{Attempts: 1})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	ran := make(chan struct{}, 1)
	w, err := NewWorker("go-fail", func(ctx context.Context, j *Job) (any, error) {
		select {
		case ran <- struct{}{}:
		default:
		}
		return nil, errors.New("boom")
	}, WithClient(client))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("job was not processed in time")
	}

	eventually(t, 3*time.Second, func() bool {
		return client.ZScore(ctx, q.keys.Failed(), job.ID).Err() == nil
	})
	if fr := client.HGet(ctx, q.keys.JobKey(job.ID), "failedReason").Val(); fr != "boom" {
		t.Errorf("failedReason = %q, want %q", fr, "boom")
	}
}
