package bullmq

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// tickCollector records the options of each job a scheduler produces, in order,
// and signals once `want` of them have been processed.
type tickCollector struct {
	mu    sync.Mutex
	opts  []map[string]any
	want  int
	ready chan struct{}
}

func newTickCollector(want int) *tickCollector {
	return &tickCollector{want: want, ready: make(chan struct{}, 1)}
}

func (c *tickCollector) record(j *Job) {
	c.mu.Lock()
	defer c.mu.Unlock()
	snapshot := make(map[string]any, len(j.opts))
	for k, v := range j.opts {
		snapshot[k] = v
	}
	c.opts = append(c.opts, snapshot)
	if len(c.opts) >= c.want {
		select {
		case c.ready <- struct{}{}:
		default:
		}
	}
}

func (c *tickCollector) wait(t *testing.T, d time.Duration) []map[string]any {
	t.Helper()
	select {
	case <-c.ready:
	case <-time.After(d):
		c.mu.Lock()
		n := len(c.opts)
		c.mu.Unlock()
		t.Fatalf("scheduler produced only %d jobs, want >= %d", n, c.want)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]map[string]any(nil), c.opts...)
}

// tickDefaults is the policy declared once on the queue. Deduplication is left out
// on purpose: one id shared by every iteration would collapse the ticks into each
// other (it is inherited honestly, which is exactly why it cannot be asserted here).
func tickDefaults() *JobOptions {
	return &JobOptions{
		Attempts:            3,
		Backoff:             &BackoffOptions{Type: "exponential", Delay: 1000},
		RemoveOnComplete:    true,
		RemoveOnFail:        map[string]any{"count": 10},
		KeepLogs:            20,
		FailParentOnFailure: true,
	}
}

// assertTickCarriesDefaults checks the options a produced iteration was stored
// with. Keys are the long form: jobFromRaw decodes the short interop keys (`kl`,
// `fpof`) back before a processor sees them.
func assertTickCarriesDefaults(t *testing.T, label string, opts map[string]any) {
	t.Helper()
	if got := toInt64(opts["attempts"]); got != 3 {
		t.Errorf("%s attempts = %d, want 3", label, got)
	}
	backoff, _ := opts["backoff"].(map[string]any)
	if backoff == nil || toStr(backoff["type"]) != "exponential" || toInt64(backoff["delay"]) != 1000 {
		t.Errorf("%s backoff = %#v, want exponential/1000", label, opts["backoff"])
	}
	if roc, _ := opts["removeOnComplete"].(bool); !roc {
		t.Errorf("%s removeOnComplete = %#v, want true", label, opts["removeOnComplete"])
	}
	rof, _ := opts["removeOnFail"].(map[string]any)
	if rof == nil || toInt64(rof["count"]) != 10 {
		t.Errorf("%s removeOnFail = %#v, want {count: 10}", label, opts["removeOnFail"])
	}
	if got := toInt64(opts["keepLogs"]); got != 20 {
		t.Errorf("%s keepLogs = %d, want 20", label, got)
	}
	if fpof, _ := opts["failParentOnFailure"].(bool); !fpof {
		t.Errorf("%s failParentOnFailure = %#v, want true", label, opts["failParentOnFailure"])
	}
}

// assertTickOverlayIsFresh checks the other half of the merge: the keys that belong
// to one iteration are overwritten, not inherited, now that the template carries the
// previous iteration's values for real (job-scheduler.ts:333-355).
//
// `repeat.count` is the one that bites — it is what the iteration limit is counted
// against, so a frozen count would make a limit unreachable. The id is asserted as a
// guard against iterations collapsing into each other, though it is the Lua that
// writes it (addJobFromScheduler). `prevMillis` is not asserted: for an interval
// scheduler the Lua recomputes the next millis from the repeat zset, so the stored
// value is only the client's guess (upstream stores none at all there — its
// nextMillis stays undefined unless a cron pattern is set).
func assertTickOverlayIsFresh(t *testing.T, ticks []map[string]any) {
	t.Helper()
	seen := map[string]bool{}
	for i, opts := range ticks {
		label := fmt.Sprintf("tick %d", i+1)
		jobID := toStr(opts["jobId"])
		if jobID == "" || seen[jobID] {
			t.Errorf("%s jobId = %q, want a fresh one", label, jobID)
		}
		seen[jobID] = true

		repeat, _ := opts["repeat"].(map[string]any)
		if got := toInt64(repeat["count"]); got != int64(i+1) {
			t.Errorf("%s repeat.count = %d, want %d", label, got, i+1)
		}
		if i == 0 {
			continue
		}
		if got, before := toInt64(opts["timestamp"]), toInt64(ticks[i-1]["timestamp"]); got < before {
			t.Errorf("%s timestamp = %d, want >= %d", label, got, before)
		}
	}
}

// Every iteration of an interval scheduler carries the queue's declared job
// options, not just the first: the worker builds the next one out of the options of
// the one it just fetched (zest-go#570).
func TestSchedulerTicksKeepJobOptions(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("go-tick-opts", WithClient(client), WithDefaultJobOptions(tickDefaults()))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	if _, err := q.UpsertJobScheduler(ctx, "every-tick", RepeatOptions{Every: 300}, "tick", nil, nil); err != nil {
		t.Fatalf("UpsertJobScheduler: %v", err)
	}

	collector := newTickCollector(3)
	w, err := NewWorker("go-tick-opts", func(ctx context.Context, j *Job) (any, error) {
		collector.record(j)
		return "ok", nil
	}, WithClient(client), WithDefaultJobOptions(tickDefaults()))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	ticks := collector.wait(t, 10*time.Second)[:3]
	for i, opts := range ticks {
		assertTickCarriesDefaults(t, fmt.Sprintf("tick %d", i+1), opts)
	}
	assertTickOverlayIsFresh(t, ticks)

	// The symptom the consumer saw: with removeOnComplete lost, every iteration
	// from the second on piles up in the completed set and leaves its hash behind.
	deadline := time.Now().Add(2 * time.Second)
	for {
		n, err := q.GetCompletedCount(ctx)
		if err != nil {
			t.Fatalf("GetCompletedCount: %v", err)
		}
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("completed count = %d, want 0 (removeOnComplete on every tick)", n)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Same for a cron schedule: the template travels the same path, only nextMillis is
// computed differently.
func TestCronSchedulerTicksKeepJobOptions(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("go-cron-tick-opts", WithClient(client), WithDefaultJobOptions(tickDefaults()))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	if _, err := q.UpsertJobScheduler(ctx, "cron-tick", RepeatOptions{Pattern: "*/1 * * * * *"}, "tick", nil, nil); err != nil {
		t.Fatalf("UpsertJobScheduler(pattern): %v", err)
	}

	collector := newTickCollector(2)
	w, err := NewWorker("go-cron-tick-opts", func(ctx context.Context, j *Job) (any, error) {
		collector.record(j)
		return "ok", nil
	}, WithClient(client), WithDefaultJobOptions(tickDefaults()))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	ticks := collector.wait(t, 12*time.Second)
	assertTickCarriesDefaults(t, "cron tick 2", ticks[1])
}

// countingTelemetry hands out a distinct trace context on every Inject, so an
// inherited one is distinguishable from a freshly injected one. pathTelemetry
// cannot tell them apart here: the context the worker reschedules under carries no
// span path, so both the first and the second iteration render as the same string.
type countingTelemetry struct {
	mu sync.Mutex
	n  int
}

func (c *countingTelemetry) StartSpan(ctx context.Context, name string, kind SpanKind) (context.Context, Span) {
	return ctx, mockSpan{}
}

func (c *countingTelemetry) Inject(ctx context.Context) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	return fmt.Sprintf("CTX-%d", c.n)
}

func (c *countingTelemetry) Extract(ctx context.Context, metadata string) context.Context {
	return ctx
}

// Deviation from upstream, deliberate: an iteration does not inherit the previous
// one's trace context. Upstream would chain them forever (its injection keeps an
// existing opts.telemetry.metadata), which for a 30s schedule means one unbounded
// trace.
func TestSchedulerTickDoesNotInheritTraceContext(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	tel := &countingTelemetry{}

	q, err := NewQueue("go-tick-tm", WithClient(client), WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	if _, err := q.UpsertJobScheduler(ctx, "tm-tick", RepeatOptions{Every: 300}, "tick", nil, nil); err != nil {
		t.Fatalf("UpsertJobScheduler: %v", err)
	}

	collector := newTickCollector(3)
	w, err := NewWorker("go-tick-tm", func(ctx context.Context, j *Job) (any, error) {
		collector.record(j)
		return "ok", nil
	}, WithClient(client), WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	ticks := collector.wait(t, 10*time.Second)
	seen := map[string]int{}
	for i, opts := range ticks[:3] {
		tm := toStr(opts["tm"])
		if tm == "" {
			t.Errorf("tick %d carries no trace context", i+1)
			continue
		}
		if prev, ok := seen[tm]; ok {
			t.Errorf("tick %d inherited the trace context of tick %d (%q)", i+1, prev, tm)
			continue
		}
		seen[tm] = i + 1
	}
}
