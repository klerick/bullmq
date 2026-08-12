package bullmq

import (
	"context"
	"testing"
	"time"
)

func TestQueueGetJob(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-getjob", WithClient(client))
	defer q.Close()

	added, err := q.Add(ctx, "task", map[string]any{"x": 1}, &JobOptions{Attempts: 2})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	got, err := q.GetJob(ctx, added.ID)
	if err != nil || got == nil {
		t.Fatalf("GetJob = %v, %v", got, err)
	}
	if got.Name != "task" || got.Data.(map[string]any)["x"] != float64(1) || got.Attempts != 2 {
		t.Errorf("GetJob returned name=%q data=%v attempts=%d", got.Name, got.Data, got.Attempts)
	}

	missing, err := q.GetJob(ctx, "does-not-exist")
	if err != nil || missing != nil {
		t.Errorf("GetJob(missing) = %v, %v; want nil, nil", missing, err)
	}
}

func TestQueueCountsAndListers(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-counts", WithClient(client))
	defer q.Close()

	_, _ = q.Add(ctx, "a", nil, nil)
	_, _ = q.Add(ctx, "b", nil, nil)
	_, _ = q.Add(ctx, "d", nil, &JobOptions{Delay: 60000})
	_, _ = q.Add(ctx, "p", nil, &JobOptions{Priority: 3})

	if n, _ := q.GetWaitingCount(ctx); n != 2 {
		t.Errorf("waiting = %d, want 2", n)
	}
	if n, _ := q.GetDelayedCount(ctx); n != 1 {
		t.Errorf("delayed = %d, want 1", n)
	}
	if n, _ := q.GetPrioritizedCount(ctx); n != 1 {
		t.Errorf("prioritized = %d, want 1", n)
	}

	counts, _ := q.GetJobCounts(ctx)
	if counts["waiting"] != 2 || counts["delayed"] != 1 || counts["prioritized"] != 1 {
		t.Errorf("GetJobCounts = %v", counts)
	}

	waiting, _ := q.GetWaiting(ctx, 0, -1)
	if len(waiting) != 2 {
		t.Errorf("GetWaiting returned %d jobs, want 2", len(waiting))
	}
	delayed, _ := q.GetDelayed(ctx, 0, -1)
	if len(delayed) != 1 {
		t.Errorf("GetDelayed returned %d jobs, want 1", len(delayed))
	}
}

func TestQueueJobStateAndPredicates(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-state", WithClient(client))
	defer q.Close()

	waitingJob, _ := q.Add(ctx, "w", nil, nil)
	delayedJob, _ := q.Add(ctx, "d", nil, &JobOptions{Delay: 60000})

	if st, _ := q.GetJobState(ctx, waitingJob.ID); st != "waiting" {
		t.Errorf("waiting job state = %q, want waiting", st)
	}
	if st, _ := q.GetJobState(ctx, delayedJob.ID); st != "delayed" {
		t.Errorf("delayed job state = %q, want delayed", st)
	}
	if ok, _ := waitingJob.IsWaiting(ctx); !ok {
		t.Error("waitingJob.IsWaiting() = false")
	}
	if ok, _ := delayedJob.IsDelayed(ctx); !ok {
		t.Error("delayedJob.IsDelayed() = false")
	}

	// Process the waiting job and confirm it becomes completed.
	done := make(chan struct{}, 1)
	w, _ := NewWorker("go-state", func(ctx context.Context, j *Job) (any, error) {
		if j.ID == waitingJob.ID {
			defer func() { done <- struct{}{} }()
		}
		return "ok", nil
	}, WithClient(client))
	defer w.Close()
	go func() { _ = w.Run(ctx) }()
	<-done

	eventually(t, 3*time.Second, func() bool {
		st, _ := q.GetJobState(ctx, waitingJob.ID)
		return st == "completed"
	})
	if n, _ := q.GetCompletedCount(ctx); n < 1 {
		t.Errorf("completed count = %d, want >= 1", n)
	}
}

func TestQueueGetCountsPerPriority(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-prio", WithClient(client))
	defer q.Close()

	_, _ = q.Add(ctx, "p0", nil, nil)
	_, _ = q.Add(ctx, "p1a", nil, &JobOptions{Priority: 1})
	_, _ = q.Add(ctx, "p1b", nil, &JobOptions{Priority: 1})
	_, _ = q.Add(ctx, "p2", nil, &JobOptions{Priority: 2})

	counts, err := q.GetCountsPerPriority(ctx, []int{0, 1, 2})
	if err != nil {
		t.Fatalf("GetCountsPerPriority: %v", err)
	}
	if len(counts) != 3 || counts[0] != 1 || counts[1] != 2 || counts[2] != 1 {
		t.Errorf("counts per priority [0,1,2] = %v, want [1 2 1]", counts)
	}

	// Priority 0 means the wait list. v5 counted the paused list instead while the
	// queue was paused; v6 dropped that branch along with the list itself, so the
	// count must not change when pausing.
	if err := q.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	counts, err = q.GetCountsPerPriority(ctx, []int{0, 1, 2})
	if err != nil {
		t.Fatalf("GetCountsPerPriority while paused: %v", err)
	}
	if len(counts) != 3 || counts[0] != 1 || counts[1] != 2 || counts[2] != 1 {
		t.Errorf("counts per priority while paused = %v, want [1 2 1]", counts)
	}
}

func TestQueueIsMaxed(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-maxed", WithClient(client))
	defer q.Close()

	// A fresh queue with no configured limit is not maxed.
	if maxed, err := q.IsMaxed(ctx); err != nil || maxed {
		t.Errorf("IsMaxed = %v, %v; want false", maxed, err)
	}
}
