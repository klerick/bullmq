package bullmq

import (
	"context"
	"errors"
	"sync/atomic"
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

// A graceful Close awaits the jobs in flight, so their locks must keep being
// renewed until the drain finishes. v0.7.1 ran the maintenance loops on the fetch
// context, which Close cancels first: renewal stopped at the start of the drain
// and a job outliving lockDuration lost its lock while still running. Another
// worker's stalled check then returned it to wait and ran it a second time, while
// the first run was still going, and that run's finalisation failed on the lock
// token. Upstream stops the lock extender only after whenCurrentJobsFinished
// (worker.ts:1402-1404), and the stalled checker after that (:1418).
func TestWorkerCloseKeepsRenewingLocksWhileDraining(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	const lockDuration = 1000 // ms; renewal every 500ms
	const hold = 3 * time.Second

	q, err := NewQueue("close-lock-renewal", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	started := make(chan struct{})
	var runs atomic.Int32
	w, err := NewWorker("close-lock-renewal", func(ctx context.Context, j *Job) (any, error) {
		runs.Add(1)
		close(started)
		time.Sleep(hold) // outlives lockDuration several times over
		return "done", nil
	}, WithClient(client), WithLockDuration(lockDuration), WithSkipStalledCheck())
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

	// A second worker, as during a rollout: it only recovers stalled jobs (it is
	// started after the job went active, so it has nothing to fetch) and never
	// renews a lock of its own.
	var hijacked atomic.Int32
	other, err := NewWorker("close-lock-renewal", func(ctx context.Context, j *Job) (any, error) {
		hijacked.Add(1)
		return "stolen", nil
	}, WithClient(client), WithLockDuration(lockDuration),
		WithStalledInterval(400), WithSkipLockRenewal())
	if err != nil {
		t.Fatalf("NewWorker(other): %v", err)
	}
	go func() { _ = other.Run(ctx) }()
	t.Cleanup(func() { _ = other.Close() })

	closeDone := make(chan error, 1)
	go func() { closeDone <- w.Close() }()

	// Well past lockDuration into the drain the lock must still be there: renewal
	// is what keeps the stalled check off a job that is simply slow.
	time.Sleep(2 * time.Second)
	lockKey := q.keys.JobKey(job.ID) + ":lock"
	exists, err := client.Exists(ctx, lockKey).Result()
	if err != nil {
		t.Fatalf("EXISTS %s: %v", lockKey, err)
	}
	if exists != 1 {
		t.Errorf("lock %s gone %s into the drain: renewal stopped too early", lockKey, 2*time.Second)
	}

	select {
	case err := <-closeDone:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(15 * time.Second):
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

	// Give the other worker's stalled check a couple more ticks to misbehave.
	time.Sleep(time.Second)
	if n := hijacked.Load(); n != 0 {
		t.Errorf("the other worker ran the job %d time(s) while it was still in flight", n)
	}
	if n := runs.Load(); n != 1 {
		t.Errorf("processor ran %d times, want 1", n)
	}

	state, err := q.GetJobState(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJobState: %v", err)
	}
	if state != "completed" {
		t.Errorf("job state = %q, want completed", state)
	}
}
