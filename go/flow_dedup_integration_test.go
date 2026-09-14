package bullmq

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// A deduplicated add is a short circuit inside Lua: the script returns the id of
// the job that already holds the key and stores nothing. The caller must get that
// id back — the locally generated one names a job that was never written.
func TestFlowAddReturnsDeduplicatedJobID(t *testing.T) {
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

	node := func() *FlowJob {
		return &FlowJob{QueueName: "dedup-single", Name: "task",
			Opts: &JobOptions{Deduplication: &DeduplicationOptions{ID: "k-single"}}}
	}
	first, err := fp.Add(ctx, node())
	if err != nil {
		t.Fatalf("first Add: %v", err)
	}
	second, err := fp.Add(ctx, node())
	if err != nil {
		t.Fatalf("second Add: %v", err)
	}

	if second.Job.ID != first.Job.ID {
		t.Errorf("second Add returned id %q, want the deduplicated job's id %q", second.Job.ID, first.Job.ID)
	}
	n, err := client.Exists(ctx, "bull:dedup-single:"+second.Job.ID).Result()
	if err != nil || n != 1 {
		t.Errorf("returned job %q exists=%d (err %v), want a job that is actually stored", second.Job.ID, n, err)
	}
}

// When the root of a tree is deduplicated it stores nothing, so its children —
// queued in the same MULTI against a parent key that will not exist — all come
// back with -5. That is the expected consequence of the short circuit, not a
// failure: the add succeeds, reports the existing root, and carries no children,
// because none were created.
func TestFlowAddWithDeduplicatedRoot(t *testing.T) {
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
		return &FlowJob{QueueName: "dedup-root", Name: "root",
			Opts:     &JobOptions{Deduplication: &DeduplicationOptions{ID: "k-tree"}},
			Children: []*FlowJob{{QueueName: "dedup-child", Name: "child"}}}
	}
	first, err := fp.Add(ctx, tree())
	if err != nil {
		t.Fatalf("first Add: %v", err)
	}
	second, err := fp.Add(ctx, tree())
	if err != nil {
		t.Fatalf("second Add: %v", err)
	}

	if second.Job.ID != first.Job.ID {
		t.Errorf("second Add returned root id %q, want the deduplicated %q", second.Job.ID, first.Job.ID)
	}
	if len(second.Children) != 0 {
		t.Errorf("second Add returned %d children, want none — they were never created", len(second.Children))
	}

	rootQ, err := NewQueue("dedup-root", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer rootQ.Close()
	childQ, err := NewQueue("dedup-child", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer childQ.Close()

	counts, err := rootQ.GetJobCounts(ctx, "waiting-children")
	if err != nil || counts["waiting-children"] != 1 {
		t.Errorf("root jobs = %v (err %v), want exactly 1 — the second add wrote nothing", counts, err)
	}
	if n, err := childQ.GetWaitingCount(ctx); err != nil || n != 1 {
		t.Errorf("child jobs = %d (err %v), want exactly 1", n, err)
	}
}

// Deduplication below the root is refused before anything is written. On a hit the
// Lua short circuit returns before linking the existing job into the new parent's
// dependencies, so the parent would sit in waiting-children with an empty
// dependency set — waiting forever, with the add reporting success.
func TestFlowAddRejectsDeduplicationBelowRoot(t *testing.T) {
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

	_, err = fp.Add(ctx, &FlowJob{QueueName: "dedup-parent", Name: "root",
		Children: []*FlowJob{{QueueName: "dedup-kid", Name: "child",
			Opts: &JobOptions{Deduplication: &DeduplicationOptions{ID: "k-child"}}}}})
	if !errors.Is(err, ErrDeduplicationNotOnRoot) {
		t.Fatalf("err = %v, want ErrDeduplicationNotOnRoot", err)
	}

	// Refused up front: the parent must not have been created either.
	keys, err := client.Keys(ctx, "bull:dedup-parent:*").Result()
	if err != nil {
		t.Fatalf("KEYS: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("rejected flow left %v behind, want nothing written", keys)
	}
}

// A -5 from a node whose parent was not deduplicated is still an error — and it
// now names the parent key, which the old errorContext dropped.
func TestFlowAddMissingParentNamesTheKey(t *testing.T) {
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

	_, err = fp.Add(ctx, &FlowJob{QueueName: "orphan-q", Name: "task",
		Opts: &JobOptions{Parent: &ParentOptions{ID: "ghost", Queue: "bull:ghost-q"}}})
	if err == nil {
		t.Fatal("Add with a missing parent succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "bull:ghost-q:ghost") {
		t.Errorf("error %q does not name the missing parent key", err)
	}
}
