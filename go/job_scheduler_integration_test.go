package bullmq

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestJobSchedulerUpsertGetRemove(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-sched", WithClient(client))
	defer q.Close()

	job, err := q.UpsertJobScheduler(ctx, "daily", RepeatOptions{Every: 60000}, "report", map[string]any{"k": "v"}, nil)
	if err != nil {
		t.Fatalf("UpsertJobScheduler: %v", err)
	}
	if !strings.HasPrefix(job.ID, "repeat:daily:") {
		t.Errorf("scheduled job id = %q, want repeat:daily:<millis>", job.ID)
	}

	if n, _ := q.GetJobSchedulersCount(ctx); n != 1 {
		t.Errorf("scheduler count = %d, want 1", n)
	}
	// The first iteration of an interval scheduler (no offset) runs immediately,
	// so it lands in wait, not delayed.
	if n, _ := q.GetWaitingCount(ctx); n != 1 {
		t.Errorf("waiting count = %d, want 1 (immediate first scheduled job)", n)
	}

	sched, err := q.GetJobScheduler(ctx, "daily")
	if err != nil || sched == nil {
		t.Fatalf("GetJobScheduler = %v, %v", sched, err)
	}
	if sched.Every != 60000 || sched.Name != "report" {
		t.Errorf("scheduler = %+v", sched)
	}

	removed, err := q.RemoveJobScheduler(ctx, "daily")
	if err != nil || !removed {
		t.Fatalf("RemoveJobScheduler = %v, %v", removed, err)
	}
	if n, _ := q.GetJobSchedulersCount(ctx); n != 0 {
		t.Errorf("scheduler count after remove = %d, want 0", n)
	}
}

// An interval scheduler keeps producing jobs: each fetch schedules the next.
func TestJobSchedulerRecurring(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-recur", WithClient(client))
	defer q.Close()

	if _, err := q.UpsertJobScheduler(ctx, "tick", RepeatOptions{Every: 300}, "tick", nil, nil); err != nil {
		t.Fatalf("UpsertJobScheduler: %v", err)
	}

	var mu sync.Mutex
	processed := 0
	enough := make(chan struct{}, 1)
	w, _ := NewWorker("go-recur", func(ctx context.Context, j *Job) (any, error) {
		mu.Lock()
		processed++
		n := processed
		mu.Unlock()
		if !strings.HasPrefix(j.ID, "repeat:tick:") {
			t.Errorf("processed job id = %q, want repeat:tick:<millis>", j.ID)
		}
		if n == 3 {
			select {
			case enough <- struct{}{}:
			default:
			}
		}
		return "ok", nil
	}, WithClient(client))
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	select {
	case <-enough:
	case <-time.After(8 * time.Second):
		mu.Lock()
		n := processed
		mu.Unlock()
		t.Fatalf("scheduler produced only %d jobs, want >= 3", n)
	}

	// The scheduler still exists after several iterations.
	if n, _ := q.GetJobSchedulersCount(ctx); n != 1 {
		t.Errorf("scheduler count = %d, want 1", n)
	}
}
