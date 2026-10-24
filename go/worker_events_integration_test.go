package bullmq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// event is one recorded listener call, so a test can assert on the sequence and
// on what each callback saw at the moment it ran (FinishedOn above all: the job is
// mutable and shared, so the value has to be captured, not read afterwards).
type event struct {
	kind         string // "active" | "completed" | "failed" | "error"
	jobID        string
	prev         string
	result       any
	err          error
	finishedOn   int64
	processedOn  int64
	attemptsMade int
}

type recorder struct {
	mu     sync.Mutex
	events []event
}

func (r *recorder) add(e event) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

func (r *recorder) snapshot() []event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]event(nil), r.events...)
}

func (r *recorder) kinds(kind string) []event {
	out := []event{}
	for _, e := range r.snapshot() {
		if e.kind == kind {
			out = append(out, e)
		}
	}
	return out
}

// listener builds a WorkerListener that records every call into r.
func (r *recorder) listener() WorkerListener {
	snap := func(kind string, job *Job) event {
		e := event{kind: kind}
		if job != nil {
			e.jobID, e.finishedOn, e.processedOn, e.attemptsMade = job.ID, job.FinishedOn, job.ProcessedOn, job.AttemptsMade
		}
		return e
	}
	return WorkerListener{
		OnActive: func(ctx context.Context, job *Job, prev string) {
			e := snap("active", job)
			e.prev = prev
			r.add(e)
		},
		OnCompleted: func(ctx context.Context, job *Job, result any, prev string) {
			e := snap("completed", job)
			e.prev, e.result = prev, result
			r.add(e)
		},
		OnFailed: func(ctx context.Context, job *Job, err error, prev string) {
			e := snap("failed", job)
			e.prev, e.err = prev, err
			r.add(e)
		},
		OnError: func(ctx context.Context, job *Job, err error) {
			e := snap("error", job)
			e.err = err
			r.add(e)
		},
	}
}

// Every failed attempt is reported, as upstream does (worker.ts:1236), and
// FinishedOn separates "will be retried" from "failed for good" — it is set only
// in moveToFailed's non-retry branch (job.ts:816). Without that distinction a
// consumer cannot tell the final failure from the two before it.
func TestWorkerListenerFailedEveryAttempt(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("ev-attempts", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	var rec recorder
	boom := errors.New("boom")
	w, err := NewWorker("ev-attempts", func(ctx context.Context, j *Job) (any, error) {
		return nil, boom
	}, WithClient(client), WithSkipStalledCheck(), WithSkipLockRenewal(),
		WithWorkerListener(rec.listener()))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	job, err := q.Add(ctx, "task", nil, &JobOptions{Attempts: 3})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	eventually(t, 15*time.Second, func() bool { return len(rec.kinds("failed")) >= 3 })

	failed := rec.kinds("failed")
	if len(failed) != 3 {
		t.Fatalf("got %d failed events, want exactly 3", len(failed))
	}
	for i, e := range failed {
		attempt := i + 1
		if e.jobID != job.ID {
			t.Errorf("attempt %d: jobID = %q, want %q", attempt, e.jobID, job.ID)
		}
		if e.prev != "active" {
			t.Errorf("attempt %d: prev = %q, want \"active\"", attempt, e.prev)
		}
		if !errors.Is(e.err, boom) {
			t.Errorf("attempt %d: err = %v, want %v", attempt, e.err, boom)
		}
		if e.attemptsMade != attempt {
			t.Errorf("attempt %d: AttemptsMade = %d, want %d", attempt, e.attemptsMade, attempt)
		}
		if e.processedOn == 0 {
			t.Errorf("attempt %d: ProcessedOn = 0, want the time the attempt started", attempt)
		}
		if attempt < 3 && e.finishedOn != 0 {
			t.Errorf("attempt %d: FinishedOn = %d, want 0 while a retry is pending", attempt, e.finishedOn)
		}
		if attempt == 3 && e.finishedOn == 0 {
			t.Errorf("attempt 3: FinishedOn = 0, want it set once the job failed for good")
		}
	}

	// Three attempts means three activations, and no job-level error.
	if n := len(rec.kinds("active")); n != 3 {
		t.Errorf("got %d active events, want 3", n)
	}
	if errs := rec.kinds("error"); len(errs) != 0 {
		t.Errorf("unexpected worker errors: %v", errs)
	}
	if state, _ := q.GetJobState(ctx, job.ID); state != "failed" {
		t.Errorf("job state = %q, want failed", state)
	}
}

// completed carries the processor's return value and a set FinishedOn
// (worker.ts:1184, job.ts:682).
func TestWorkerListenerCompleted(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("ev-completed", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	var rec recorder
	w, err := NewWorker("ev-completed", func(ctx context.Context, j *Job) (any, error) {
		return "sent", nil
	}, WithClient(client), WithSkipStalledCheck(), WithSkipLockRenewal(),
		WithWorkerListener(rec.listener()))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	job, err := q.Add(ctx, "task", nil, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	eventually(t, 15*time.Second, func() bool { return len(rec.kinds("completed")) == 1 })

	e := rec.kinds("completed")[0]
	if e.jobID != job.ID {
		t.Errorf("jobID = %q, want %q", e.jobID, job.ID)
	}
	if e.result != "sent" {
		t.Errorf("result = %v, want \"sent\"", e.result)
	}
	if e.prev != "active" {
		t.Errorf("prev = %q, want \"active\"", e.prev)
	}
	if e.finishedOn == 0 {
		t.Error("FinishedOn = 0, want it set on a completed job")
	}
	if e.processedOn == 0 {
		t.Error("ProcessedOn = 0, want the time processing started")
	}
	if e.attemptsMade != 1 {
		t.Errorf("AttemptsMade = %d, want 1", e.attemptsMade)
	}

	// active comes before completed, and reports the state it came from.
	all := rec.snapshot()
	if len(all) < 2 || all[0].kind != "active" || all[1].kind != "completed" {
		t.Fatalf("event order = %v, want active then completed", all)
	}
	if all[0].prev != "waiting" {
		t.Errorf("active prev = %q, want \"waiting\"", all[0].prev)
	}
	if all[0].finishedOn != 0 {
		t.Errorf("active FinishedOn = %d, want 0 — the job has not finished yet", all[0].finishedOn)
	}
}

// The failParentOnFailure cascade fails the parent in the worker, not in Lua: the
// script only records the reason and moves the parent to wait. The parent's
// processor never runs, so the listener is the only way a consumer learns of it.
// Upstream emits failed here too (worker.ts:1076-1084).
func TestWorkerListenerFailedOnDeferredFailure(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	fp, err := NewFlowProducer(WithClient(client))
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}
	defer fp.Close()

	parentQ, err := NewQueue("ev-parent", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer parentQ.Close()

	var rec recorder
	// The parent's worker must never reach its processor.
	processorRan := make(chan struct{}, 1)
	parentW, err := NewWorker("ev-parent", func(ctx context.Context, j *Job) (any, error) {
		processorRan <- struct{}{}
		return nil, nil
	}, WithClient(client), WithSkipStalledCheck(), WithSkipLockRenewal(),
		WithWorkerListener(rec.listener()))
	if err != nil {
		t.Fatalf("NewWorker(parent): %v", err)
	}
	defer parentW.Close()

	childW, err := NewWorker("ev-child", func(ctx context.Context, j *Job) (any, error) {
		return nil, errors.New("child exploded")
	}, WithClient(client), WithSkipStalledCheck(), WithSkipLockRenewal())
	if err != nil {
		t.Fatalf("NewWorker(child): %v", err)
	}
	defer childW.Close()

	tree, err := fp.Add(ctx, &FlowJob{
		QueueName: "ev-parent", Name: "parent",
		Children: []*FlowJob{{
			QueueName: "ev-child", Name: "child",
			Opts: &JobOptions{FailParentOnFailure: true},
		}},
	})
	if err != nil {
		t.Fatalf("FlowProducer.Add: %v", err)
	}

	go func() { _ = childW.Run(ctx) }()
	go func() { _ = parentW.Run(ctx) }()

	eventually(t, 15*time.Second, func() bool { return len(rec.kinds("failed")) == 1 })

	e := rec.kinds("failed")[0]
	if e.jobID != tree.Job.ID {
		t.Errorf("jobID = %q, want the parent %q", e.jobID, tree.Job.ID)
	}
	var ue *UnrecoverableError
	if !errors.As(e.err, &ue) {
		t.Errorf("err = %T(%v), want *UnrecoverableError", e.err, e.err)
	}
	if e.err == nil || !containsStr(e.err.Error(), "failed") {
		t.Errorf("err = %v, want the cascade reason", e.err)
	}
	if e.finishedOn == 0 {
		t.Error("FinishedOn = 0: a cascaded failure is final, no attempts are left")
	}
	if e.prev != "active" {
		t.Errorf("prev = %q, want \"active\"", e.prev)
	}

	select {
	case <-processorRan:
		t.Error("the parent's processor ran; the cascade must fail it without processing")
	default:
	}
}

func containsStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// Parking a job for its children is not a failure: upstream returns before
// moveToFailed and emits nothing (worker.ts:1213-1232, the WaitingChildrenError
// branch). A consumer that marks its domain entity failed on this event must not
// see one here.
//
// Shaped like TestWaitingChildrenIsNotASpanError: the processor returns the
// sentinel, which is how a processor signals "I parked myself". A flow parent
// cannot be used — it sits in waiting-children while a child is pending, so its
// worker never gets to run it in the first place.
func TestWorkerListenerNoFailedOnWaitingChildren(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("ev-waiting-children", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	var rec recorder
	parked := make(chan struct{}, 1)
	w, err := NewWorker("ev-waiting-children", func(ctx context.Context, j *Job) (any, error) {
		parked <- struct{}{}
		return nil, ErrWaitingChildren
	}, WithClient(client), WithSkipStalledCheck(), WithSkipLockRenewal(),
		WithWorkerListener(rec.listener()))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	if _, err := q.Add(ctx, "task", nil, nil); err != nil {
		t.Fatalf("Add: %v", err)
	}
	select {
	case <-parked:
	case <-time.After(15 * time.Second):
		t.Fatal("the processor never ran")
	}
	time.Sleep(500 * time.Millisecond) // give a stray event time to show up

	if n := len(rec.kinds("failed")); n != 0 {
		t.Errorf("got %d failed events for a job waiting on children, want 0", n)
	}
	if n := len(rec.kinds("completed")); n != 0 {
		t.Errorf("got %d completed events for a parked job, want 0", n)
	}
	if n := len(rec.kinds("active")); n != 1 {
		t.Errorf("got %d active events, want 1 — the job did run", n)
	}
	if errs := rec.kinds("error"); len(errs) != 0 {
		t.Errorf("unexpected worker errors: %v", errs)
	}
}

// A handler does side effects, so a rollout must not cut it off: it runs in the
// job's goroutine, which Close's drain awaits (v0.7.2).
func TestWorkerCloseWaitsForListener(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("ev-close", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	w, err := NewWorker("ev-close", func(ctx context.Context, j *Job) (any, error) {
		return nil, errors.New("boom")
	}, WithClient(client), WithSkipStalledCheck(), WithSkipLockRenewal(),
		WithWorkerListener(WorkerListener{
			OnFailed: func(ctx context.Context, job *Job, err error, prev string) {
				close(entered)
				<-release // a slow side effect
				close(finished)
			},
		}))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	go func() { _ = w.Run(ctx) }()

	if _, err := q.Add(ctx, "task", nil, nil); err != nil {
		t.Fatalf("Add: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(15 * time.Second):
		t.Fatal("the failed handler never ran")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- w.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned (%v) while the handler was still running", err)
	case <-time.After(400 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return after the handler finished")
	}
	select {
	case <-finished:
	default:
		t.Error("Close returned before the handler had finished")
	}
}

// A listener is the consumer's code doing side effects; a panic in it must not
// take the worker's process down. It is reported to OnError instead, and the job's
// own outcome stands.
func TestWorkerListenerPanicGoesToOnError(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("ev-panic", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	errs := make(chan error, 4)
	w, err := NewWorker("ev-panic", func(ctx context.Context, j *Job) (any, error) {
		return "ok", nil
	}, WithClient(client), WithSkipStalledCheck(), WithSkipLockRenewal(),
		WithWorkerListener(WorkerListener{
			OnCompleted: func(ctx context.Context, job *Job, result any, prev string) {
				panic("handler exploded")
			},
			OnError: func(ctx context.Context, job *Job, err error) { errs <- err },
		}))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	job, err := q.Add(ctx, "task", nil, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	select {
	case err := <-errs:
		if !containsStr(err.Error(), "handler exploded") {
			t.Errorf("OnError got %v, want the panic value", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the recovered panic was never reported to OnError")
	}

	// The job itself still completed: the panic happened after finalisation.
	if state, _ := q.GetJobState(ctx, job.ID); state != "completed" {
		t.Errorf("job state = %q, want completed", state)
	}
}

// A finalisation that cannot be written to Redis used to vanish into `_ =`. It is
// now reported: upstream throws out of handleCompleted and retryIfFailed turns it
// into an 'error' event (worker.ts:1593). Reproduced the way it actually happens
// in production — the worker lost the job's lock, so moveToFinished refuses the
// write — which is exactly the case the consumer could not see before.
func TestWorkerListenerErrorOnFailedFinalisation(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("ev-finalise", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	var rec recorder
	lockGone := make(chan struct{})
	started := make(chan string, 1)
	w, err := NewWorker("ev-finalise", func(ctx context.Context, j *Job) (any, error) {
		started <- j.ID
		<-lockGone // the lock is pulled out from under this job
		return "ok", nil
	}, WithClient(client), WithSkipStalledCheck(), WithSkipLockRenewal(),
		WithWorkerListener(rec.listener()))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	if _, err := q.Add(ctx, "task", nil, nil); err != nil {
		t.Fatalf("Add: %v", err)
	}
	var jobID string
	select {
	case jobID = <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("the job never started")
	}
	if err := client.Del(ctx, q.keys.JobKey(jobID)+":lock").Err(); err != nil {
		t.Fatalf("DEL lock: %v", err)
	}
	close(lockGone)

	eventually(t, 10*time.Second, func() bool { return len(rec.kinds("error")) == 1 })

	e := rec.kinds("error")[0]
	if e.jobID != jobID {
		t.Errorf("error jobID = %q, want %q", e.jobID, jobID)
	}
	if !containsStr(e.err.Error(), "to completed") {
		t.Errorf("error = %v, want it to name the finalisation that failed", e.err)
	}
	// And no success was reported for a write that never landed.
	if n := len(rec.kinds("completed")); n != 0 {
		t.Errorf("got %d completed events for a job whose finalisation failed, want 0", n)
	}
}

// A job must leave the lock-renewal set before it is finalised, as upstream does
// (untrackJob sits inside every retryIfFailed closure in processJob, ahead of
// handleFailed/handleCompleted — worker.ts:1083, :1111, :1130, :1143).
//
// Before callbacks that window was microseconds wide, so keeping the job
// registered until the goroutine returned was harmless. A listener widens it to
// however long the handler runs. The damage: moveToFailed with attempts left puts
// the job straight back in wait, this same worker picks it up under a new token,
// and registerActive overwrites the map entry keyed by the job id — then the first
// goroutine's deferred unregister deletes the *new* entry. The second run loses
// lock renewal, the stalled check returns it to wait and runs it a third time while
// the second is still going.
func TestWorkerListenerDoesNotStripTheRetrysLockRenewal(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("ev-retry-lock", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	var runs atomic.Int32
	var rec recorder
	// The handler of the first failure outlives lockDuration, which is what used to
	// take the retry's renewal down with it when it finally returned.
	slowFailed := make(chan struct{}, 1)
	listener := rec.listener()
	inner := listener.OnFailed
	listener.OnFailed = func(ctx context.Context, job *Job, err error, prev string) {
		inner(ctx, job, err, prev)
		if len(rec.kinds("failed")) == 1 {
			slowFailed <- struct{}{}
			time.Sleep(2 * time.Second) // > lockDuration
		}
	}

	w, err := NewWorker("ev-retry-lock", func(ctx context.Context, j *Job) (any, error) {
		switch runs.Add(1) {
		case 1:
			return nil, errors.New("boom") // one attempt left, so this retries at once
		case 2:
			time.Sleep(5 * time.Second) // must stay locked for all of it
			return "ok", nil
		default:
			return "late", nil // a third run means the renewal was lost
		}
	}, WithClient(client), WithConcurrency(2),
		WithLockDuration(1000), WithStalledInterval(500),
		WithWorkerListener(listener))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	job, err := q.Add(ctx, "task", nil, &JobOptions{Attempts: 2})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	select {
	case <-slowFailed:
	case <-time.After(20 * time.Second):
		t.Fatal("the first attempt never failed")
	}

	eventually(t, 30*time.Second, func() bool {
		state, _ := q.GetJobState(ctx, job.ID)
		return state == "completed"
	})
	time.Sleep(time.Second) // let a stray third run show up

	if n := runs.Load(); n != 2 {
		t.Errorf("processor ran %d times, want 2 — a third run means the retry lost its lock", n)
	}
	// The second run's own finalisation landed, so nothing was reported as an error.
	if errs := rec.kinds("error"); len(errs) != 0 {
		t.Errorf("unexpected worker errors: %v", errs)
	}
	if n := len(rec.kinds("completed")); n != 1 {
		t.Errorf("got %d completed events, want 1", n)
	}
}

// The other half of the contract, pinned directly: a finalised job is out of the
// renewal set *before* its listener runs, as upstream untracks ahead of each
// handler (worker.ts:1083-1143). The token guard in unregisterActive is what keeps
// a retry's renewal alive (TestWorkerListenerDoesNotStripTheRetrysLockRenewal);
// this is the ordering itself, which otherwise leaves the worker renewing the lock
// of a job that is already finished for as long as the handler runs.
//
// Concurrency 1 on purpose: no other run can be registering the same job id while
// the handler is inside, so the only thing that can have cleared this run's token
// is the finalising path.
func TestWorkerUnregistersBeforeTheListenerRuns(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("ev-untrack", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	var holder atomic.Pointer[Worker] // the listener needs the worker it belongs to
	type seen struct{ stillRegistered bool }
	failedSeen := make(chan seen, 1)
	completedSeen := make(chan seen, 1)

	registered := func(job *Job) bool {
		w := holder.Load()
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.active[job.ID] == job.token
	}

	var runs atomic.Int32
	w, err := NewWorker("ev-untrack", func(ctx context.Context, j *Job) (any, error) {
		if runs.Add(1) == 1 {
			return nil, NewUnrecoverableError("no retries for this one")
		}
		return "ok", nil
	}, WithClient(client), WithConcurrency(1), WithSkipStalledCheck(),
		WithWorkerListener(WorkerListener{
			OnFailed: func(ctx context.Context, job *Job, err error, prev string) {
				failedSeen <- seen{registered(job)}
			},
			OnCompleted: func(ctx context.Context, job *Job, result any, prev string) {
				completedSeen <- seen{registered(job)}
			},
		}))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	holder.Store(w)
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	if _, err := q.Add(ctx, "fails", nil, nil); err != nil {
		t.Fatalf("Add: %v", err)
	}
	select {
	case s := <-failedSeen:
		if s.stillRegistered {
			t.Error("the job was still in the lock-renewal set when OnFailed ran")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("OnFailed never ran")
	}

	if _, err := q.Add(ctx, "succeeds", nil, nil); err != nil {
		t.Fatalf("Add: %v", err)
	}
	select {
	case s := <-completedSeen:
		if s.stillRegistered {
			t.Error("the job was still in the lock-renewal set when OnCompleted ran")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("OnCompleted never ran")
	}
}
