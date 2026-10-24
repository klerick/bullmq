package bullmq

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Upstream validates maxStalledCount as "a number >= 0" (worker.ts:318-321), so a
// negative value is a construction error rather than something clamped silently.
func TestNewWorkerRejectsNegativeMaxStalledCount(t *testing.T) {
	client := requireRedis(t)
	w, err := NewWorker("msc-invalid", func(ctx context.Context, j *Job) (any, error) { return nil, nil },
		WithClient(client), WithMaxStalledCount(-1))
	if err == nil {
		_ = w.Close()
		t.Fatal("NewWorker accepted maxStalledCount = -1, want an error")
	}
	if !errors.Is(err, ErrInvalidMaxStalledCount) {
		t.Errorf("err = %v, want ErrInvalidMaxStalledCount", err)
	}
	if w != nil {
		t.Error("NewWorker returned a worker alongside the error")
	}
}

// 0 is a valid value upstream and means "do not recover a stalled job at all":
// moveStalledJobsToWait-9.lua:96 fails the job as soon as stalledCount (1) exceeds
// it. The port used to read a zero as "not set" and substitute the default 1, so
// this configuration was unreachable.
func TestMaxStalledCountZeroFailsTheJobAtOnce(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("msc-zero", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	hang := make(chan struct{})
	var runs atomic.Int32
	// Concurrency 2: the first job hangs, and the loop still needs a free slot to
	// pick the job up again once the stalled check has returned it to wait.
	w, err := NewWorker("msc-zero", func(ctx context.Context, j *Job) (any, error) {
		if runs.Add(1) == 1 {
			<-hang // never renews its lock, so it stalls
			return nil, nil
		}
		return "ok", nil
	}, WithClient(client), WithConcurrency(2), WithSkipLockRenewal(),
		WithLockDuration(1000), WithStalledInterval(500), WithMaxStalledCount(0))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	// Declared after the Close defer so it runs before it: Close drains the jobs in
	// flight, and the hanging one has to be let go first or the drain never ends.
	defer close(hang)
	go func() { _ = w.Run(ctx) }()

	job, err := q.Add(ctx, "task", nil, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	eventually(t, 20*time.Second, func() bool {
		state, _ := q.GetJobState(ctx, job.ID)
		return state == "failed"
	})

	stored, err := q.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if !strings.Contains(stored.FailedReason, "stalled more than allowable limit") {
		t.Errorf("FailedReason = %q, want the stalled-limit reason", stored.FailedReason)
	}
	// The job was never handed to the processor a second time: with a limit of 0
	// the first stalled check fails it instead of recovering it.
	if n := runs.Load(); n != 1 {
		t.Errorf("processor ran %d times, want 1", n)
	}
}

// The control: with the option left out the default is still 1, so the first
// stalled detection recovers the job rather than failing it.
func TestMaxStalledCountDefaultRecoversOnce(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("msc-default", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	hang := make(chan struct{})
	var runs atomic.Int32
	w, err := NewWorker("msc-default", func(ctx context.Context, j *Job) (any, error) {
		if runs.Add(1) == 1 {
			<-hang
			return nil, nil
		}
		return "ok", nil
	}, WithClient(client), WithConcurrency(2), WithSkipLockRenewal(),
		WithLockDuration(1000), WithStalledInterval(500))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	// Declared after the Close defer so it runs before it: Close drains the jobs in
	// flight, and the hanging one has to be let go first or the drain never ends.
	defer close(hang)
	go func() { _ = w.Run(ctx) }()

	job, err := q.Add(ctx, "task", nil, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	eventually(t, 20*time.Second, func() bool {
		state, _ := q.GetJobState(ctx, job.ID)
		return state == "completed"
	})

	if n := runs.Load(); n != 2 {
		t.Errorf("processor ran %d times, want 2 — one stall and one recovery", n)
	}
}
