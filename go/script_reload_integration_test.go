package bullmq

import (
	"context"
	"sync"
	"testing"
	"time"
)

// A Redis restart (or SCRIPT FLUSH, or a failover) empties the script cache. Flow
// adds run inside MULTI, where go-redis cannot fall back from EVALSHA to EVAL, so
// they used to fail with NOSCRIPT from then on — for the life of the process,
// because the preload was guarded by a sync.Once. The producer must recover by
// itself instead.
func TestFlowAddSurvivesScriptFlush(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	fp, err := NewFlowProducer(WithClient(client))
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}
	defer fp.Close()

	tree := func() *FlowJob {
		return &FlowJob{
			QueueName: "reload-parent", Name: "parent",
			Children: []*FlowJob{{QueueName: "reload-child", Name: "child"}},
		}
	}
	if _, err := fp.Add(ctx, tree()); err != nil {
		t.Fatalf("first Add: %v", err)
	}

	if err := client.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("script flush: %v", err)
	}

	if _, err := fp.Add(ctx, tree()); err != nil {
		t.Fatalf("Add after SCRIPT FLUSH: %v", err)
	}
}

// The recovery retries the whole transaction, so it must not enqueue anything
// twice: flow nodes keep the ids assigned on the first attempt, and the Lua add
// scripts treat a known id as a duplicate instead of a second job.
func TestFlowAddAfterFlushDoesNotDuplicateJobs(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	fp, err := NewFlowProducer(WithClient(client))
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}
	defer fp.Close()

	// Warm the cache, then drop it: the very next add takes the recovery path.
	if _, err := fp.Add(ctx, &FlowJob{QueueName: "dup-warm", Name: "warm"}); err != nil {
		t.Fatalf("warm-up Add: %v", err)
	}
	if err := client.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("script flush: %v", err)
	}

	if _, err := fp.Add(ctx, &FlowJob{
		QueueName: "dup-parent", Name: "parent",
		Children: []*FlowJob{
			{QueueName: "dup-child", Name: "child-a"},
			{QueueName: "dup-child", Name: "child-b"},
		},
	}); err != nil {
		t.Fatalf("Add after SCRIPT FLUSH: %v", err)
	}

	parentQ, err := NewQueue("dup-parent", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer parentQ.Close()
	childQ, err := NewQueue("dup-child", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer childQ.Close()

	if n, err := parentQ.GetJobCounts(ctx, "waiting-children"); err != nil || n["waiting-children"] != 1 {
		t.Errorf("parent jobs = %v (err %v), want exactly 1", n, err)
	}
	if n, err := childQ.GetWaitingCount(ctx); err != nil || n != 2 {
		t.Errorf("child jobs = %d (err %v), want exactly 2", n, err)
	}
}

// AddBulk runs scripts in a pipeline too. It used to survive a flush only because
// it re-sent all 49 SCRIPT LOADs on every call; now it shares the cache, so this
// pins that it still recovers.
func TestAddBulkSurvivesScriptFlush(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("reload-bulk", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	specs := []BulkJob{{Name: "a"}, {Name: "b"}}
	if _, err := q.AddBulk(ctx, specs); err != nil {
		t.Fatalf("first AddBulk: %v", err)
	}
	if err := client.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("script flush: %v", err)
	}
	jobs, err := q.AddBulk(ctx, specs)
	if err != nil {
		t.Fatalf("AddBulk after SCRIPT FLUSH: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("AddBulk returned %d jobs, want 2", len(jobs))
	}
	if n, err := q.GetWaitingCount(ctx); err != nil || n != 4 {
		t.Errorf("waiting = %d (err %v), want 4 — two adds of two jobs, none lost or doubled", n, err)
	}
}

// Every goroutine in flight when the cache dies gets NOSCRIPT at once. They must
// share one reload sweep rather than each firing 49 SCRIPT LOADs.
func TestConcurrentFlowAddsReloadScriptsOnce(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	fp, err := NewFlowProducer(WithClient(client))
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}
	defer fp.Close()

	if _, err := fp.Add(ctx, &FlowJob{QueueName: "conc-warm", Name: "warm"}); err != nil {
		t.Fatalf("warm-up Add: %v", err)
	}
	before := fp.conn.scriptCache.generation()
	if err := client.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("script flush: %v", err)
	}

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = fp.Add(ctx, &FlowJob{QueueName: "conc-parent", Name: "parent",
				Children: []*FlowJob{{QueueName: "conc-child", Name: "child"}}})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent Add %d: %v", i, err)
		}
	}
	if got := fp.conn.scriptCache.generation() - before; got != 1 {
		t.Errorf("scripts reloaded %d times, want exactly 1 shared reload", got)
	}
}
