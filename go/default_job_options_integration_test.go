package bullmq

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// storedOpts reads a job's effective options back out of Redis, which is where the
// merge has to be visible: the scripts and every other port read this hash.
func storedOpts(t *testing.T, client *redis.Client, jobKey string) map[string]any {
	t.Helper()
	raw, err := client.HGet(context.Background(), jobKey, "opts").Result()
	if err != nil {
		t.Fatalf("HGet %s opts: %v", jobKey, err)
	}
	var opts map[string]any
	if err := json.Unmarshal([]byte(raw), &opts); err != nil {
		t.Fatalf("unmarshal opts of %s: %v", jobKey, err)
	}
	return opts
}

// A queue's defaultJobOptions fill in what the call left unset and lose to what it
// set, per key (queue.ts:381-385).
func TestQueueAddMergesDefaultJobOptions(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("defaults-add", WithClient(client), WithDefaultJobOptions(&JobOptions{
		Attempts:         3,
		RemoveOnComplete: true,
		Backoff:          &BackoffOptions{Type: "exponential", Delay: 1000},
		KeepLogs:         10,
	}))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}

	job, err := q.Add(ctx, "task", map[string]any{"k": 1}, &JobOptions{Attempts: 1, Priority: 5})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	opts := storedOpts(t, client, q.keys.JobKey(job.ID))
	if opts["attempts"] != float64(1) {
		t.Errorf("attempts = %v, want 1 from the call", opts["attempts"])
	}
	if opts["priority"] != float64(5) {
		t.Errorf("priority = %v, want 5 from the call", opts["priority"])
	}
	if opts["removeOnComplete"] != true {
		t.Errorf("removeOnComplete = %v, want true from the defaults", opts["removeOnComplete"])
	}
	// keepLogs travels under its short interop key (packer.go optsEncodeMap).
	if opts["kl"] != float64(10) {
		t.Errorf("kl = %v, want 10 from the defaults", opts["kl"])
	}
	backoff, _ := opts["backoff"].(map[string]any)
	if backoff["type"] != "exponential" || backoff["delay"] != float64(1000) {
		t.Errorf("backoff = %v, want the defaults' exponential/1000", opts["backoff"])
	}

	// The job struct the caller gets back must agree with what was stored.
	if job.Attempts != 1 {
		t.Errorf("job.Attempts = %d, want 1", job.Attempts)
	}
}

// Adding with no options at all is the common case for a queue that declares its
// policy once; the defaults must still apply.
func TestQueueAddWithoutOptionsUsesDefaults(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("defaults-nil-opts", WithClient(client),
		WithDefaultJobOptions(&JobOptions{Attempts: 4, RemoveOnFail: 20}))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}

	job, err := q.Add(ctx, "task", nil, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	opts := storedOpts(t, client, q.keys.JobKey(job.ID))
	if opts["attempts"] != float64(4) || opts["removeOnFail"] != float64(20) {
		t.Errorf("opts = %v, want the defaults applied", opts)
	}
}

// A job id in the defaults must not apply: it would collide every job of the queue
// onto one. Upstream re-assigns jobId from the call after the spread.
func TestQueueDefaultsDoNotProvideJobID(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("defaults-jobid", WithClient(client),
		WithDefaultJobOptions(&JobOptions{JobID: "fixed", Attempts: 2}))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}

	first, err := q.Add(ctx, "task", nil, nil)
	if err != nil {
		t.Fatalf("first Add: %v", err)
	}
	second, err := q.Add(ctx, "task", nil, nil)
	if err != nil {
		t.Fatalf("second Add: %v", err)
	}
	if first.ID == "fixed" || first.ID == second.ID {
		t.Errorf("job ids %q and %q, want two distinct generated ids", first.ID, second.ID)
	}
	if mine, err := q.Add(ctx, "task", nil, &JobOptions{JobID: "mine"}); err != nil || mine.ID != "mine" {
		t.Errorf("explicit job id = %v (err %v), want %q", mine, err, "mine")
	}
}

// AddBulk merges per job, including the ones added with no options
// (queue.ts:441-446).
func TestQueueAddBulkMergesDefaultJobOptions(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("defaults-bulk", WithClient(client),
		WithDefaultJobOptions(&JobOptions{Attempts: 3, RemoveOnComplete: true}))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}

	jobs, err := q.AddBulk(ctx, []BulkJob{
		{Name: "a", Opts: &JobOptions{Priority: 7}},
		{Name: "b"},
	})
	if err != nil {
		t.Fatalf("AddBulk: %v", err)
	}

	first := storedOpts(t, client, q.keys.JobKey(jobs[0].ID))
	if first["attempts"] != float64(3) || first["priority"] != float64(7) || first["removeOnComplete"] != true {
		t.Errorf("first job opts = %v, want defaults merged with priority 7", first)
	}
	second := storedOpts(t, client, q.keys.JobKey(jobs[1].ID))
	if second["attempts"] != float64(3) || second["removeOnComplete"] != true {
		t.Errorf("second job opts = %v, want the defaults applied", second)
	}
}

// A scheduler's template is merged with the defaults too (queue.ts:497), so every
// iteration it produces carries them.
func TestQueueUpsertJobSchedulerMergesDefaultJobOptions(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("defaults-sched", WithClient(client),
		WithDefaultJobOptions(&JobOptions{Attempts: 5, RemoveOnComplete: true}))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}

	job, err := q.UpsertJobScheduler(ctx, "every-min", RepeatOptions{Every: 60000}, "tick", nil,
		&JobOptions{Priority: 2})
	if err != nil {
		t.Fatalf("UpsertJobScheduler: %v", err)
	}

	opts := storedOpts(t, client, q.keys.JobKey(job.ID))
	if opts["attempts"] != float64(5) || opts["removeOnComplete"] != true || opts["priority"] != float64(2) {
		t.Errorf("scheduled job opts = %v, want the defaults merged with priority 2", opts)
	}
	// The stored template drives every later iteration, so the defaults have to be
	// in it rather than only in the first produced job.
	tmpl, err := client.HGet(ctx, q.keys.Repeat()+":every-min", "opts").Result()
	if err != nil {
		t.Fatalf("HGet scheduler opts: %v", err)
	}
	var stored map[string]any
	if err := json.Unmarshal([]byte(tmpl), &stored); err != nil {
		t.Fatalf("unmarshal scheduler opts: %v", err)
	}
	if stored["attempts"] != float64(5) || stored["removeOnComplete"] != true {
		t.Errorf("stored scheduler template = %v, want the defaults in it", stored)
	}
}

// Per-queue defaults reach every node of a flow by its queue name, the node's own
// options win, and a queue without an entry is untouched
// (flow-producer.ts:399-437).
func TestFlowAddAppliesQueuesOptionsPerQueue(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
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

	tree, err := fp.Add(ctx, &FlowJob{
		QueueName: "flowdef-root", Name: "root",
		Children: []*FlowJob{
			{QueueName: "flowdef-child", Name: "own", Opts: &JobOptions{Attempts: 9}},
			{QueueName: "flowdef-plain", Name: "plain"},
		},
	}, FlowOpts{QueuesOptions: map[string]FlowQueueOptions{
		"flowdef-root":  {DefaultJobOptions: &JobOptions{Attempts: 7, RemoveOnComplete: true}},
		"flowdef-child": {DefaultJobOptions: &JobOptions{Attempts: 2, Priority: 4}},
	}})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	root := storedOpts(t, client, "bull:flowdef-root:"+tree.Job.ID)
	if root["attempts"] != float64(7) || root["removeOnComplete"] != true {
		t.Errorf("root opts = %v, want its queue's defaults", root)
	}

	child := storedOpts(t, client, "bull:flowdef-child:"+tree.Children[0].Job.ID)
	if child["attempts"] != float64(9) {
		t.Errorf("child attempts = %v, want 9 from the node's own options", child["attempts"])
	}
	if child["priority"] != float64(4) {
		t.Errorf("child priority = %v, want 4 from its queue's defaults", child["priority"])
	}
	if _, ok := child["removeOnComplete"]; ok {
		t.Errorf("child opts = %v, want no removeOnComplete — that default belongs to another queue", child)
	}

	plain := storedOpts(t, client, "bull:flowdef-plain:"+tree.Children[1].Job.ID)
	if plain["attempts"] != float64(0) || plain["priority"] != nil {
		t.Errorf("plain node opts = %v, want nothing applied for a queue with no entry", plain)
	}
}

// The parent link is structural: it comes from the tree walk and from nowhere
// else. Upstream writes `parent: parent?.parentOpts` over whatever the merge
// produced (flow-producer.ts:430), so a Parent sitting in a queue's defaults must
// not turn a root into somebody's child.
func TestFlowAddQueuesOptionsNeverProvideParent(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
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

	defaults := &JobOptions{Attempts: 2, Parent: &ParentOptions{ID: "999", Queue: "bull:somewhere"}}
	tree, err := fp.Add(ctx, &FlowJob{
		QueueName: "flowpar-root", Name: "root",
		Children: []*FlowJob{{QueueName: "flowpar-child", Name: "leaf"}},
	}, FlowOpts{QueuesOptions: map[string]FlowQueueOptions{
		"flowpar-root":  {DefaultJobOptions: defaults},
		"flowpar-child": {DefaultJobOptions: defaults},
	}})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	rootKey, err := client.HGet(ctx, "bull:flowpar-root:"+tree.Job.ID, "parentKey").Result()
	if err == nil && rootKey != "" {
		t.Errorf("root parentKey = %q, want none", rootKey)
	}
	childKey, err := client.HGet(ctx, "bull:flowpar-child:"+tree.Children[0].Job.ID, "parentKey").Result()
	if err != nil {
		t.Fatalf("HGet child parentKey: %v", err)
	}
	if childKey != "bull:flowpar-root:"+tree.Job.ID {
		t.Errorf("child parentKey = %q, want the tree's parent", childKey)
	}
	// The rest of the defaults still applied.
	if got := storedOpts(t, client, "bull:flowpar-child:"+tree.Children[0].Job.ID); got["attempts"] != float64(2) {
		t.Errorf("child attempts = %v, want 2 from its queue's defaults", got["attempts"])
	}
}

// The two flow limits on deduplication are checked before anything is written, so
// they have to see the merged options: a deduplication id arriving from a queue's
// defaults strands the tree exactly as an explicit one would.
func TestFlowAddRejectsDeduplicationFromQueuesOptions(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
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
		return &FlowJob{QueueName: "flowdedup-root", Name: "root",
			Children: []*FlowJob{{QueueName: "flowdedup-child", Name: "leaf"}}}
	}

	_, err = fp.Add(ctx, tree(), FlowOpts{QueuesOptions: map[string]FlowQueueOptions{
		"flowdedup-child": {DefaultJobOptions: &JobOptions{Deduplication: &DeduplicationOptions{ID: "k"}}},
	}})
	if !errors.Is(err, ErrDeduplicationNotOnRoot) {
		t.Errorf("err = %v, want ErrDeduplicationNotOnRoot", err)
	}

	_, err = fp.Add(ctx, tree(), FlowOpts{QueuesOptions: map[string]FlowQueueOptions{
		"flowdedup-root": {DefaultJobOptions: &JobOptions{
			Deduplication: &DeduplicationOptions{ID: "k", Replace: true}}},
	}})
	if !errors.Is(err, ErrDeduplicationReplaceInFlow) {
		t.Errorf("err = %v, want ErrDeduplicationReplaceInFlow", err)
	}

	if n, _ := client.Exists(ctx, "bull:flowdedup-root:1", "bull:flowdedup-child:1").Result(); n != 0 {
		t.Errorf("%d job keys exist, want a rejected flow to write nothing", n)
	}
}

// The variadic is upstream's optional second argument, not a list: more than one
// is a programming mistake and is reported rather than silently half-applied.
func TestFlowAddRejectsMoreThanOneFlowOpts(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()

	fp, err := NewFlowProducer(WithClient(client))
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}
	defer fp.Close()

	_, err = fp.Add(ctx, &FlowJob{QueueName: "flowtoomany", Name: "root"}, FlowOpts{}, FlowOpts{})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("err = %v, want ErrInvalidConfig", err)
	}
}

// WithDefaultJobOptions is a Queue option. FlowProducer has no defaults of its own
// in any port — per-queue options are the whole mechanism — so passing the same
// option slice to NewFlowProducer must change nothing rather than quietly apply
// one queue's policy to every node of every tree.
func TestFlowProducerIgnoresWithDefaultJobOptions(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	fp, err := NewFlowProducer(WithClient(client),
		WithDefaultJobOptions(&JobOptions{Attempts: 6, RemoveOnComplete: true}))
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}
	defer fp.Close()

	tree, err := fp.Add(ctx, &FlowJob{QueueName: "flowignore", Name: "root"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	opts := storedOpts(t, client, "bull:flowignore:"+tree.Job.ID)
	if opts["attempts"] != float64(0) || opts["removeOnComplete"] != nil {
		t.Errorf("node opts = %v, want a producer-level default to have no effect", opts)
	}
}
