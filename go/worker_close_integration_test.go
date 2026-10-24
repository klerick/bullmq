package bullmq

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Close must stop a running loop, not just drop the clients under it. It used to
// do only the latter: every fetch then failed against the closed client, the loop
// retried every 100ms, and Run never returned while the caller's context stayed
// alive. Upstream fixed the same shape in worker.ts (stop retrying once closing).
//
// It must also be prompt: the fetch is parked in a blocking read on the marker,
// and cancelling the derived context aborts it instead of waiting out the timeout.
func TestWorkerCloseStopsRunLoop(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	w, err := NewWorker("close-loop", func(ctx context.Context, j *Job) (any, error) { return nil, nil },
		WithClient(client), WithSkipStalledCheck(), WithSkipLockRenewal())
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	time.Sleep(300 * time.Millisecond) // let the loop park on the marker

	// The caller's context stays alive on purpose: Close alone has to stop the loop.
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return within 3s of Close")
	}
}

// Close is called from defers and cleanups, often more than once; the second call
// must not panic on the already-closed signal.
func TestWorkerCloseIsIdempotent(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	w, err := NewWorker("close-twice", func(ctx context.Context, j *Job) (any, error) { return nil, nil },
		WithClient(client), WithSkipStalledCheck(), WithSkipLockRenewal())
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	go func() { _ = w.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	if err := w.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
