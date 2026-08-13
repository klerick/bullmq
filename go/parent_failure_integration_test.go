package bullmq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// startTestWorker runs a worker for the duration of the test.
func startTestWorker(t *testing.T, ctx context.Context, client *redis.Client, queue string, proc Processor) *Worker {
	t.Helper()
	w, err := NewWorker(queue, proc, WithClient(client))
	if err != nil {
		t.Fatalf("NewWorker(%s): %v", queue, err)
	}
	t.Cleanup(func() { _ = w.Close() })
	go func() { _ = w.Run(ctx) }()
	return w
}

// failingProcessor fails every job it receives.
func failingProcessor(ctx context.Context, j *Job) (any, error) { return nil, errors.New("boom") }

// parentFailureFixture builds a flow whose parent waits on the given children and
// starts a worker on the parent queue that only records whether it ran.
type parentFailureFixture struct {
	tree        *JobNode
	parentQueue *Queue
	parentRuns  *int32
}

func newParentFailureFixture(t *testing.T, ctx context.Context, client *redis.Client, name string, children []*FlowJob) parentFailureFixture {
	t.Helper()
	fp, err := NewFlowProducer(WithClient(client))
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}
	t.Cleanup(func() { _ = fp.Close() })

	tree, err := fp.Add(ctx, &FlowJob{QueueName: name, Name: "parent", Children: children})
	if err != nil {
		t.Fatalf("flow Add: %v", err)
	}
	q, err := NewQueue(name, WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	var runs int32
	startTestWorker(t, ctx, client, name, func(ctx context.Context, j *Job) (any, error) {
		atomic.AddInt32(&runs, 1)
		return "parent-done", nil
	})
	return parentFailureFixture{tree: tree, parentQueue: q, parentRuns: &runs}
}

// failParentOnFailure: a child that exhausts its attempts must take the parent
// down with it. The cascade is split in two — Lua parks the parent back in wait
// with a "defa" reason, and the worker turns that into an unrecoverable failure
// without ever running the processor.
func TestChildFailureFailsParentWithFpof(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	f := newParentFailureFixture(t, ctx, client, "fpof-parent", []*FlowJob{{
		QueueName: "fpof-child", Name: "child",
		Opts: &JobOptions{Attempts: 1, FailParentOnFailure: true},
	}})
	startTestWorker(t, ctx, client, "fpof-child", failingProcessor)

	parentID := f.tree.Job.ID
	eventually(t, 10*time.Second, func() bool {
		state, _ := f.parentQueue.GetJobState(ctx, parentID)
		return state == "failed"
	})
	if runs := atomic.LoadInt32(f.parentRuns); runs != 0 {
		t.Errorf("parent processor ran %d times, want 0 (deferred failure must skip it)", runs)
	}
	want := "child bull:fpof-child:" + f.tree.Children[0].Job.ID + " failed"
	if got := client.HGet(ctx, f.parentQueue.keys.JobKey(parentID), "failedReason").Val(); got != want {
		t.Errorf("parent failedReason = %q, want %q", got, want)
	}
}

// The upstream default with no policy flag: a failed child is left in the
// parent's dependencies, so the parent waits forever. Pinned deliberately — it
// is the behaviour the four options exist to change.
func TestChildFailureLeavesParentWaitingByDefault(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	f := newParentFailureFixture(t, ctx, client, "nopolicy-parent", []*FlowJob{{
		QueueName: "nopolicy-child", Name: "child",
		Opts: &JobOptions{Attempts: 1},
	}})
	startTestWorker(t, ctx, client, "nopolicy-child", failingProcessor)

	childID := f.tree.Children[0].Job.ID
	childQueue, err := NewQueue("nopolicy-child", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer childQueue.Close()
	eventually(t, 10*time.Second, func() bool {
		state, _ := childQueue.GetJobState(ctx, childID)
		return state == "failed"
	})

	// Give the (non-existent) cascade a chance to happen before asserting.
	time.Sleep(500 * time.Millisecond)
	state, err := f.parentQueue.GetJobState(ctx, f.tree.Job.ID)
	if err != nil {
		t.Fatalf("GetJobState: %v", err)
	}
	if state != "waiting-children" {
		t.Errorf("parent state = %q, want %q", state, "waiting-children")
	}
	if runs := atomic.LoadInt32(f.parentRuns); runs != 0 {
		t.Errorf("parent processor ran %d times, want 0", runs)
	}
}

// continueParentOnFailure: the parent starts as soon as any child fails, even
// while a sibling is still pending.
func TestChildFailureContinuesParentWithCpof(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	f := newParentFailureFixture(t, ctx, client, "cpof-parent", []*FlowJob{
		{QueueName: "cpof-child", Name: "child-fail",
			Opts: &JobOptions{Attempts: 1, ContinueParentOnFailure: true}},
		// No worker on this queue: the sibling stays pending on purpose.
		{QueueName: "cpof-pending", Name: "child-pending"},
	})
	startTestWorker(t, ctx, client, "cpof-child", failingProcessor)

	eventually(t, 10*time.Second, func() bool {
		return atomic.LoadInt32(f.parentRuns) > 0
	})
	failedChildren := client.HGetAll(ctx, f.parentQueue.keys.JobKey(f.tree.Job.ID)+":failed").Val()
	childKey := "bull:cpof-child:" + f.tree.Children[0].Job.ID
	if failedChildren[childKey] != "boom" {
		t.Errorf("parent failed-children = %v, want %q -> %q", failedChildren, childKey, "boom")
	}
}

// ignoreDependencyOnFailure: the failed child stops blocking the parent but is
// still recorded in the parent's failed-children hash.
func TestChildFailureIgnoredWithIdof(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	f := newParentFailureFixture(t, ctx, client, "idof-parent", []*FlowJob{
		{QueueName: "idof-child", Name: "child-fail",
			Opts: &JobOptions{Attempts: 1, IgnoreDependencyOnFailure: true}},
		{QueueName: "idof-ok", Name: "child-ok"},
	})
	startTestWorker(t, ctx, client, "idof-child", failingProcessor)
	startTestWorker(t, ctx, client, "idof-ok", func(ctx context.Context, j *Job) (any, error) {
		return "ok", nil
	})

	eventually(t, 10*time.Second, func() bool {
		return atomic.LoadInt32(f.parentRuns) > 0
	})
	failedChildren := client.HGetAll(ctx, f.parentQueue.keys.JobKey(f.tree.Job.ID)+":failed").Val()
	childKey := "bull:idof-child:" + f.tree.Children[0].Job.ID
	if failedChildren[childKey] != "boom" {
		t.Errorf("parent failed-children = %v, want %q -> %q", failedChildren, childKey, "boom")
	}
}

// removeDependencyOnFailure: same unblocking as idof, but the child leaves no
// trace in the parent's failed-children hash.
func TestChildFailureRemovesDependencyWithRdof(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	f := newParentFailureFixture(t, ctx, client, "rdof-parent", []*FlowJob{
		{QueueName: "rdof-child", Name: "child-fail",
			Opts: &JobOptions{Attempts: 1, RemoveDependencyOnFailure: true}},
		{QueueName: "rdof-ok", Name: "child-ok"},
	})
	startTestWorker(t, ctx, client, "rdof-child", failingProcessor)
	startTestWorker(t, ctx, client, "rdof-ok", func(ctx context.Context, j *Job) (any, error) {
		return "ok", nil
	})

	eventually(t, 10*time.Second, func() bool {
		return atomic.LoadInt32(f.parentRuns) > 0
	})
	if n := client.Exists(ctx, f.parentQueue.keys.JobKey(f.tree.Job.ID)+":failed").Val(); n != 0 {
		t.Errorf("parent failed-children hash exists, want none (rdof records nothing)")
	}
}

// The Extra escape hatch must drive the real cascade now, not just land in opts:
// this is the exact workaround consumers wrote while the typed fields were
// missing, and it used to be silently inert.
func TestChildFailureFailsParentViaExtraFlag(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	f := newParentFailureFixture(t, ctx, client, "extra-parent", []*FlowJob{{
		QueueName: "extra-child", Name: "child",
		Opts: &JobOptions{Attempts: 1, Extra: map[string]any{"failParentOnFailure": true}},
	}})
	startTestWorker(t, ctx, client, "extra-child", failingProcessor)

	eventually(t, 10*time.Second, func() bool {
		state, _ := f.parentQueue.GetJobState(ctx, f.tree.Job.ID)
		return state == "failed"
	})
}
