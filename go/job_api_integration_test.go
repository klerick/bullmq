package bullmq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestJobUpdateProgressAndData(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-jobupd", WithClient(client))
	defer q.Close()

	job, _ := q.Add(ctx, "task", map[string]any{"a": 1}, nil)

	if err := job.UpdateProgress(ctx, 50); err != nil {
		t.Fatalf("UpdateProgress: %v", err)
	}
	if p := client.HGet(ctx, q.keys.JobKey(job.ID), "progress").Val(); p != "50" {
		t.Errorf("stored progress = %q, want 50", p)
	}

	if err := job.UpdateData(ctx, map[string]any{"a": 2, "b": 3}); err != nil {
		t.Fatalf("UpdateData: %v", err)
	}
	if d := client.HGet(ctx, q.keys.JobKey(job.ID), "data").Val(); d != `{"a":2,"b":3}` {
		t.Errorf("stored data = %q, want {\"a\":2,\"b\":3}", d)
	}
}

func TestJobLog(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-joblog", WithClient(client))
	defer q.Close()

	job, _ := q.Add(ctx, "task", nil, nil)
	if _, err := job.Log(ctx, "first"); err != nil {
		t.Fatalf("Log: %v", err)
	}
	if _, err := job.Log(ctx, "second"); err != nil {
		t.Fatalf("Log: %v", err)
	}
	logs, err := job.GetLogs(ctx, 0, -1)
	if err != nil {
		t.Fatalf("GetLogs: %v", err)
	}
	if len(logs) != 2 || logs[0] != "first" || logs[1] != "second" {
		t.Errorf("logs = %v, want [first second]", logs)
	}
}

func TestJobPromoteAndChangeDelay(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-promote", WithClient(client))
	defer q.Close()

	job, _ := q.Add(ctx, "later", nil, &JobOptions{Delay: 60000})

	if err := job.ChangeDelay(ctx, 90000); err != nil {
		t.Fatalf("ChangeDelay: %v", err)
	}
	if d := client.HGet(ctx, q.keys.JobKey(job.ID), "delay").Val(); d != "90000" {
		t.Errorf("delay after ChangeDelay = %q, want 90000", d)
	}

	if err := job.Promote(ctx); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if n, _ := q.GetDelayedCount(ctx); n != 0 {
		t.Errorf("delayed after Promote = %d, want 0", n)
	}
	if n, _ := q.GetWaitingCount(ctx); n != 1 {
		t.Errorf("waiting after Promote = %d, want 1", n)
	}
}

func TestJobChangePriority(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-changeprio", WithClient(client))
	defer q.Close()

	job, _ := q.Add(ctx, "task", nil, nil) // starts in wait
	if err := job.ChangePriority(ctx, 5, false); err != nil {
		t.Fatalf("ChangePriority: %v", err)
	}
	if n, _ := q.GetPrioritizedCount(ctx); n != 1 {
		t.Errorf("prioritized after ChangePriority = %d, want 1", n)
	}
	if n, _ := q.GetWaitingCount(ctx); n != 0 {
		t.Errorf("waiting after ChangePriority = %d, want 0", n)
	}
}

func TestJobRetry(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-jobretry", WithClient(client))
	defer q.Close()

	added, _ := q.Add(ctx, "doomed", nil, &JobOptions{Attempts: 1})
	ran := make(chan struct{}, 1)
	w, _ := NewWorker("go-jobretry", func(ctx context.Context, j *Job) (any, error) {
		select {
		case ran <- struct{}{}:
		default:
		}
		return nil, errors.New("boom")
	}, WithClient(client))
	go func() { _ = w.Run(ctx) }()
	<-ran
	eventually(t, 3*time.Second, func() bool { n, _ := q.GetFailedCount(ctx); return n == 1 })
	_ = w.Close()

	job, _ := q.GetJob(ctx, added.ID)
	if err := job.Retry(ctx, "failed"); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if n, _ := q.GetFailedCount(ctx); n != 0 {
		t.Errorf("failed after Retry = %d, want 0", n)
	}
	if n, _ := q.GetWaitingCount(ctx); n != 1 {
		t.Errorf("waiting after Retry = %d, want 1", n)
	}
}

func TestJobRemove(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-jobremove", WithClient(client))
	defer q.Close()

	job, _ := q.Add(ctx, "task", nil, nil)
	if err := job.Remove(ctx, true); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if got, _ := q.GetJob(ctx, job.ID); got != nil {
		t.Error("job present after Remove")
	}
}
