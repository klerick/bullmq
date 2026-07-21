package bullmq

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// extendLock renews a lock only for the holder of the current token.
func TestExtendLock(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("go-lock", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()
	job, err := q.Add(ctx, "task", nil, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Fetch into active, which sets the lock to our token.
	if _, _, _, _, err := q.scripts.moveToActive(ctx, moveToActiveOpts{token: "tok1", lockDuration: 5000}); err != nil {
		t.Fatalf("moveToActive: %v", err)
	}
	lockKey := q.keys.JobKey(job.ID) + ":lock"
	if v := client.Get(ctx, lockKey).Val(); v != "tok1" {
		t.Fatalf("lock = %q, want %q", v, "tok1")
	}

	ok, err := q.scripts.extendLock(ctx, job.ID, "tok1", 8000)
	if err != nil || !ok {
		t.Errorf("extendLock with holder token = %v (err %v), want true", ok, err)
	}
	ok, err = q.scripts.extendLock(ctx, job.ID, "not-the-holder", 8000)
	if err != nil || ok {
		t.Errorf("extendLock with wrong token = %v (err %v), want false", ok, err)
	}
}

// moveStalledJobsToWait needs two cycles: the first marks active jobs as
// potentially stalled, the second recovers those still active (whose worker never
// renewed the lock) back to wait.
func TestMoveStalledJobsToWait(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("go-stall", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()
	job, err := q.Add(ctx, "task", nil, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Simulate a worker that grabbed the job and died: fetch into active with a
	// short lock and never renew or finish it.
	if _, _, _, _, err := q.scripts.moveToActive(ctx, moveToActiveOpts{token: "dead", lockDuration: 50}); err != nil {
		t.Fatalf("moveToActive: %v", err)
	}
	time.Sleep(120 * time.Millisecond) // let the lock expire

	// The check is throttled with SET stalled-check PX stalledInterval NX, so
	// stalledInterval must be > 0 and we must wait it out between cycles.
	const stalledInterval int64 = 50
	s1, err := q.scripts.moveStalledJobsToWait(ctx, 3, stalledInterval)
	if err != nil {
		t.Fatalf("moveStalledJobsToWait cycle 1: %v", err)
	}
	if len(s1) != 0 {
		t.Errorf("cycle 1 should report no stalled jobs yet, got %v", s1)
	}

	time.Sleep(time.Duration(stalledInterval+20) * time.Millisecond) // let the throttle expire

	s2, err := q.scripts.moveStalledJobsToWait(ctx, 3, stalledInterval)
	if err != nil {
		t.Fatalf("moveStalledJobsToWait cycle 2: %v", err)
	}
	if len(s2) != 1 || s2[0] != job.ID {
		t.Fatalf("cycle 2 should recover job %s, got %v", job.ID, s2)
	}

	// The job should be back in wait and out of active.
	if n := client.LLen(ctx, q.keys.Active()).Val(); n != 0 {
		t.Errorf("active list len = %d, want 0", n)
	}
	if pos, err := client.LPos(ctx, q.keys.Wait(), job.ID, redis.LPosArgs{}).Result(); err != nil || pos < 0 {
		t.Errorf("job not found in wait after recovery (pos=%d, err=%v)", pos, err)
	}
}

// A job that runs longer than its lock duration completes exactly once: the
// worker's lock-renewal ticker keeps the stalled checker from re-queuing it.
func TestWorkerRenewsLockForLongJob(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("go-longjob", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()
	job, err := q.Add(ctx, "long", nil, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	var mu sync.Mutex
	runs := 0
	done := make(chan struct{}, 1)
	w, err := NewWorker("go-longjob", func(ctx context.Context, j *Job) (any, error) {
		mu.Lock()
		runs++
		mu.Unlock()
		time.Sleep(800 * time.Millisecond) // longer than lockDuration
		select {
		case done <- struct{}{}:
		default:
		}
		return "ok", nil
	}, WithClient(client),
		WithLockDuration(200),    // renew ticker fires every 100ms
		WithStalledInterval(150), // stalled checker runs while the job is processing
		WithMaxStalledCount(1),
	)
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("long job did not finish in time")
	}
	time.Sleep(400 * time.Millisecond) // allow any erroneous re-queue to surface

	eventually(t, 3*time.Second, func() bool {
		return client.ZScore(ctx, q.keys.Completed(), job.ID).Err() == nil
	})
	mu.Lock()
	defer mu.Unlock()
	if runs != 1 {
		t.Errorf("processor ran %d times, want 1 (lock renewal should prevent a stalled re-queue)", runs)
	}
}
