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
		// A Close that leaves the caller's context alive is a clean stop, so Run
		// reports no error; context.Canceled is reserved for the caller giving up
		// (pinned by TestWorkerRunReportsCancelledContext).
		if err != nil {
			t.Errorf("Run returned %v, want nil after a graceful close", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return within 3s of Close")
	}
}

// The other half of the contract: when the caller cancels, Run says so.
func TestWorkerRunReportsCancelledContext(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithCancel(context.Background())

	w, err := NewWorker("close-cancel", func(ctx context.Context, j *Job) (any, error) { return nil, nil },
		WithClient(client), WithSkipStalledCheck(), WithSkipLockRenewal())
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
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

// Close must let a job that is already being processed finish, and only then drop
// the clients — otherwise the job's own finalisation fails against a closed client
// and it sits active until the stalled check recovers it. Upstream's close awaits
// processing the same way.
func TestWorkerCloseWaitsForJobInFlight(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("close-drain", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	w, err := NewWorker("close-drain", func(ctx context.Context, j *Job) (any, error) {
		close(started)
		<-release // hold the job in flight; a graceful close must not abort it
		return "done", nil
	}, WithClient(client), WithSkipStalledCheck(), WithSkipLockRenewal())
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	job, err := q.Add(ctx, "task", nil, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the job never started")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- w.Close() }()

	// While the job is held, Close has to be waiting, not returning.
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned (%v) while a job was still in flight", err)
	case <-time.After(400 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return after the job finished")
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run returned %v, want nil after a graceful close", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Close")
	}

	// The job finished for real: its finalisation ran before the clients went away.
	state, err := q.GetJobState(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJobState: %v", err)
	}
	if state != "completed" {
		t.Errorf("job state = %q, want completed", state)
	}
}
