package bullmq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultLockDuration    int64 = 30000 // ms
	defaultStalledInterval int64 = 30000 // ms
	defaultMaxStalledCount       = 1
	defaultDrainDelay            = 5 * time.Second
	maxBlockTimeout              = 10 * time.Second

	// minimumBlockTimeout floors how long the worker blocks on the marker, so a
	// wake-up that is already due still yields to Redis instead of spinning.
	// python/bullmq/worker.py uses 1ms when the server can block for 1ms (Redis >= 7)
	// and 2ms otherwise; the fork has no capability probe, so it takes the 1ms value.
	minimumBlockTimeout = time.Millisecond
)

// Processor handles a single job. Returning a value completes the job; returning
// an error fails it (subject to the retry policy). Return ErrWaitingChildren to
// leave the job in the waiting-children state without finalising it.
type Processor func(ctx context.Context, job *Job) (any, error)

// Worker fetches jobs from a queue and runs them through a Processor. Construct it
// with NewWorker and the same functional options as Queue, plus WithConcurrency /
// WithLockDuration.
type Worker struct {
	queue                *Queue
	processor            Processor
	concurrency          int
	lockDuration         int64
	stalledInterval      int64
	maxStalledCount      int
	limiter              *Limiter
	workerName           string
	skipStalledCheck     bool
	skipLockRenewal      bool
	metricsMaxDataPoints int
	drainDelay           time.Duration
	id                   string
	listener             WorkerListener

	drained    bool
	blockUntil int64

	// closed is shut by Close to stop a running Run loop. Without it the loop only
	// noticed a cancelled context, so Close against a live context left it retrying
	// a closed client every 100ms forever (upstream fixed the same shape in
	// worker.ts: stop retrying once closing).
	closeOnce sync.Once
	closeErr  error
	closed    chan struct{} // shut by Close: stop fetching
	runDone   chan struct{} // closed by Run on exit, so Close can await the drain
	running   atomic.Bool   // whether Run was ever started

	mu     sync.Mutex
	active map[string]string // jobID -> lock token, for lock renewal
}

// NewWorker builds a worker for the named queue. It does not start processing;
// call Run.
func NewWorker(name string, processor Processor, opts ...Option) (*Worker, error) {
	q, err := NewQueue(name, opts...)
	if err != nil {
		return nil, err
	}
	cfg := newConfig(opts...)
	concurrency := cfg.concurrency
	if concurrency < 1 {
		concurrency = 1
	}
	lockDuration := cfg.lockDuration
	if lockDuration == 0 {
		lockDuration = defaultLockDuration
	}
	stalledInterval := cfg.stalledInterval
	if stalledInterval == 0 {
		stalledInterval = defaultStalledInterval
	}
	// The default applies only when the option was not given: 0 is a value in its
	// own right, not an absent one.
	maxStalledCount := defaultMaxStalledCount
	if cfg.maxStalledCount != nil {
		if *cfg.maxStalledCount < 0 {
			return nil, fmt.Errorf("%w, got %d", ErrInvalidMaxStalledCount, *cfg.maxStalledCount)
		}
		maxStalledCount = *cfg.maxStalledCount
	}
	return &Worker{
		queue:                q,
		processor:            processor,
		concurrency:          concurrency,
		lockDuration:         lockDuration,
		stalledInterval:      stalledInterval,
		maxStalledCount:      maxStalledCount,
		limiter:              cfg.limiter,
		workerName:           cfg.workerName,
		skipStalledCheck:     cfg.skipStalledCheck,
		skipLockRenewal:      cfg.skipLockRenewal,
		metricsMaxDataPoints: cfg.metricsMaxDataPoints,
		drainDelay:           defaultDrainDelay,
		id:                   genID(),
		listener:             cfg.listener,
		drained:              true,
		active:               make(map[string]string),
		closed:               make(chan struct{}),
		runDone:              make(chan struct{}),
	}, nil
}

// Run processes jobs until ctx is cancelled. It blocks, so callers typically run
// it in a goroutine. In-flight jobs are awaited before Run returns.
func (w *Worker) Run(ctx context.Context) error {
	w.running.Store(true)
	defer close(w.runDone) // lets Close wait for the drain below

	// Close cancels fetching only: the context handed to processors stays the
	// caller's, so jobs already running are awaited rather than aborted (upstream's
	// non-forced close behaves the same). Cancelling the fetch context also aborts
	// the blocking marker read instead of waiting out its timeout.
	fetchCtx, stopFetch := context.WithCancel(ctx)
	defer stopFetch()
	// Maintenance outlives fetching. Close stops fetching but awaits the jobs in
	// flight, and those jobs need their locks renewed for as long as they run:
	// a lock lost mid-job lets another worker's stalled check return it to wait
	// and run it a second time. So the loops hang off the caller's context, not
	// the fetch context, and are stopped after the drain below — upstream's close
	// clears the lock extender only after whenCurrentJobsFinished (worker.ts:1402-1404)
	// and the stalled checker after that (:1418). Cancelling the caller's context
	// is the force path and still stops everything at once.
	maintCtx, stopMaint := context.WithCancel(ctx)
	defer stopMaint()
	go func() {
		select {
		case <-w.closed:
			stopFetch()
		case <-fetchCtx.Done():
		}
	}()

	// Register a client name so Queue.GetWorkers can discover this worker
	// (best-effort; some managed Redis providers reject CLIENT SETNAME).
	w.ensureClientName(ctx)

	sem := make(chan struct{}, w.concurrency)
	// Two groups, because the drain is ordered: the jobs are awaited first, the
	// maintenance loops are only then told to stop.
	var jobs sync.WaitGroup
	var maint sync.WaitGroup

	// Background maintenance: renew locks of active jobs, and recover stalled ones.
	if !w.skipLockRenewal {
		maint.Add(1)
		go func() { defer maint.Done(); w.lockRenewalLoop(maintCtx) }()
	}
	if !w.skipStalledCheck {
		maint.Add(1)
		go func() { defer maint.Done(); w.stalledCheckLoop(maintCtx) }()
	}

	seq := 0

fetching:
	for fetchCtx.Err() == nil {
		select {
		case sem <- struct{}{}:
		case <-fetchCtx.Done():
			break fetching
		}

		seq++
		token := w.id + ":" + strconv.Itoa(seq)
		job, err := w.getNextJob(fetchCtx, token)
		if err != nil {
			<-sem
			if fetchCtx.Err() != nil {
				break fetching
			}
			time.Sleep(100 * time.Millisecond) // bounded retry on transient error
			continue
		}
		if job == nil {
			<-sem
			continue
		}

		jobs.Add(1)
		go func(job *Job) {
			defer jobs.Done()
			defer func() { <-sem }()
			w.processJob(ctx, job)
		}(job)
	}

	jobs.Wait()  // locks still being renewed while this runs
	stopMaint()  // only now, as upstream does after whenCurrentJobsFinished
	maint.Wait() // no maintenance call in flight when the clients are released
	return runExitErr(ctx)
}

// runExitErr distinguishes the two ways the loop ends: a cancelled caller context
// is reported as such, while a Close that left the context alive is a clean stop.
func runExitErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// getNextJob returns the next job, blocking on the marker when the queue is drained.
func (w *Worker) getNextJob(ctx context.Context, token string) (*Job, error) {
	if w.drained {
		w.waitForJob(ctx)
	}
	job, err := w.moveToActive(ctx, token)
	if err != nil {
		return nil, err
	}
	if job == nil {
		w.drained = true
		return nil, nil
	}
	w.drained = false
	return job, nil
}

func (w *Worker) moveToActive(ctx context.Context, token string) (*Job, error) {
	var limiter any
	if w.limiter != nil {
		limiter = map[string]any{"max": w.limiter.Max, "duration": w.limiter.Duration}
	}
	jobData, jobID, limitUntil, delayUntil, err := w.queue.scripts.moveToActive(ctx, moveToActiveOpts{
		token:        token,
		lockDuration: w.lockDuration,
		limiter:      limiter,
	})
	if err != nil {
		return nil, err
	}
	if jobData == nil {
		// A rate-limit or delayed-job wait: block until the queue can serve again.
		if limitUntil > 0 {
			w.blockUntil = nowMillis() + limitUntil
		} else if delayUntil > 0 {
			w.blockUntil = delayUntil
		}
		return nil, nil
	}
	job := jobFromRaw(w.queue, jobData, jobID)
	job.token = token

	// A job produced by a scheduler triggers the next iteration up-front, mirroring
	// the Node worker (it schedules the next run right after fetching the current).
	if isJobScheduler(job.RepeatJobKey) {
		repeat := repeatFromOpts(job.opts["repeat"])
		_, _ = w.queue.upsertJobScheduler(ctx, job.RepeatJobKey, repeat, job.Name, job.Data, jobOptionsFromMap(job.opts), false, job.ID)
	}
	return job, nil
}

// waitForJob blocks on the marker (BZPOPMIN) using a dedicated client, so a job
// added by any producer wakes the worker promptly.
//
// Below one second the command is built by hand rather than through go-redis's typed
// BZPopMin: that helper formats the timeout as whole seconds, which rounds a
// sub-second wait up to 1s and logs a warning for every delayed job. At or above one
// second the typed helper stays, because it also sets the per-command read timeout
// (unexported, so the raw path cannot) that a long block needs to keep the socket
// from timing out first.
func (w *Worker) waitForJob(ctx context.Context) {
	timeout := w.blockTimeout()
	marker := w.queue.keys.Marker()
	if timeout < time.Second {
		_ = w.queue.conn.blockingClient.Process(ctx, newBZPopMinCmd(ctx, marker, timeout))
	} else {
		_, _ = w.queue.conn.blockingClient.BZPopMin(ctx, timeout, marker).Result()
	}
	w.blockUntil = 0
}

// newBZPopMinCmd builds the same command go-redis's BZPopMin builds, but keeps the
// timeout as fractional seconds, which Redis >= 6 accepts.
func newBZPopMinCmd(ctx context.Context, marker string, timeout time.Duration) *redis.ZWithKeyCmd {
	return redis.NewZWithKeyCmd(ctx, "bzpopmin", marker, timeout.Seconds())
}

// blockTimeout is the time to wait on the marker: until the next delayed job or the
// end of a rate-limit window when one is pending, the drain delay otherwise, floored
// at minimumBlockTimeout. Ported from python/bullmq/worker.py::getBlockTimeout.
func (w *Worker) blockTimeout() time.Duration {
	if w.blockUntil > 0 {
		d := time.Duration(w.blockUntil-nowMillis()) * time.Millisecond
		if d > maxBlockTimeout {
			return maxBlockTimeout
		}
		if d < minimumBlockTimeout {
			return minimumBlockTimeout
		}
		return d
	}
	if w.drainDelay < minimumBlockTimeout {
		return minimumBlockTimeout
	}
	return w.drainDelay
}

func (w *Worker) processJob(ctx context.Context, job *Job) {
	w.registerActive(job.ID, job.token)
	// A catch-all for the paths that finalise nothing (a job parking itself for its
	// children): the finalising paths drop the job earlier, see finishCompleted and
	// finishFailed. Upstream has the same pair — untrackJob ahead of each handler
	// plus one in its finally block (worker.ts:1083-1143).
	defer w.unregisterActive(job.ID, job.token)

	fo := finishOpts{
		token:            job.token,
		lockDuration:     w.lockDuration,
		removeOnComplete: job.opts["removeOnComplete"],
		removeOnFail:     job.opts["removeOnFail"],
	}
	if w.metricsMaxDataPoints > 0 {
		fo.maxMetricsSize = strconv.Itoa(w.metricsMaxDataPoints)
	}

	// Continue the trace started when the job was added (context travels in `tm`).
	pctx := w.queue.tel.extract(ctx, toStr(job.opts["tm"]))
	pctx, span := w.queue.tel.start(pctx, SpanKindConsumer, "process")
	span.setAttrs(map[string]any{"bullmq.queue": w.queue.name, "bullmq.job.name": job.Name, "bullmq.job.id": job.ID})

	// 'active' is reported here, in the job's own goroutine, and not where upstream
	// emits it (worker.ts:1040, in the fetch path): a listener runs inline, and
	// inline in the fetch loop one slow handler would stall fetching for every
	// other job. The state reported is the same, and here the drain covers it.
	w.emitActive(pctx, job)

	// Finalising runs inside the process context so the complete/fail span nests
	// under this one, as upstream's handleCompleted/handleFailed do inside the
	// 'process' span (worker.ts:931-1029).
	if job.DeferredFailure != "" {
		// A job carrying a deferred failure (a child failed with failParentOnFailure)
		// must not run: the cascade already decided its outcome, the worker only
		// executes it. Ported from worker.ts getUnrecoverableErrorMessage.
		deferred := NewUnrecoverableError(job.DeferredFailure)
		w.finishFailed(pctx, job, deferred, fo)
		span.finish(deferred)
		return
	}

	result, procErr := w.processor(pctx, job)
	if procErr != nil {
		if errors.Is(procErr, ErrWaitingChildren) {
			// Parking a job for its children is not a failure: upstream returns
			// before moveToFailed and records nothing (worker.ts:1093-1107).
			span.finish(nil)
			return
		}
		w.finishFailed(pctx, job, procErr, fo)
		span.finish(procErr)
		return
	}
	w.finishCompleted(pctx, job, result, fo)
	span.finish(nil)
}

// finishCompleted writes the job's success, then reports it. The order is
// upstream's: the event comes after the move (worker.ts:1181-1184), so a listener
// sees the state that is already in Redis, with Job.FinishedOn set.
func (w *Worker) finishCompleted(ctx context.Context, job *Job, result any, fo finishOpts) {
	// Out of the renewal set before the write, as upstream does: past this point the
	// job is finished, and a listener must not hold a slot in there for as long as it
	// runs. See unregisterActive for what goes wrong if it does.
	w.unregisterActive(job.ID, job.token)
	if err := job.moveToCompleted(ctx, result, fo, false); err != nil {
		// Upstream throws out of handleCompleted and retryIfFailed turns it into
		// an 'error' event (worker.ts:1593). This port has no finalisation retry
		// loop yet, so it reports and gives up on this attempt; the job stays
		// active and the stalled check recovers it.
		w.reportError(ctx, job, fmt.Errorf("bullmq: move job %s to completed: %w", job.ID, err))
		return
	}
	w.emitCompleted(ctx, job, result)
}

// finishFailed applies the retry policy and then reports the failed attempt, as
// upstream does after moveToFailed (worker.ts:1234-1236). The event fires on every
// failed attempt, not only the last: Job.FinishedOn, set only once no retry is
// left, is what separates the two.
func (w *Worker) finishFailed(ctx context.Context, job *Job, jobErr error, fo finishOpts) {
	// Before the write, as in finishCompleted — and it matters most here: moveToFailed
	// with attempts left puts the job back in wait straight away, so the next run can
	// register itself while this one is still reporting the failure.
	w.unregisterActive(job.ID, job.token)
	if err := job.moveToFailed(ctx, jobErr, fo, false); err != nil {
		w.reportError(ctx, job, fmt.Errorf("bullmq: move job %s to failed: %w", job.ID, err))
		return
	}
	w.emitFailed(ctx, job, jobErr)
}

// The prev argument of each event is the state the job came from, exactly the
// literal upstream passes (worker.ts:1040, :1184, :1236).
func (w *Worker) emitActive(ctx context.Context, job *Job) {
	if w.listener.OnActive == nil {
		return
	}
	w.guard(ctx, job, "active", func() { w.listener.OnActive(ctx, job, "waiting") })
}

func (w *Worker) emitCompleted(ctx context.Context, job *Job, result any) {
	if w.listener.OnCompleted == nil {
		return
	}
	w.guard(ctx, job, "completed", func() { w.listener.OnCompleted(ctx, job, result, "active") })
}

func (w *Worker) emitFailed(ctx context.Context, job *Job, jobErr error) {
	if w.listener.OnFailed == nil {
		return
	}
	w.guard(ctx, job, "failed", func() { w.listener.OnFailed(ctx, job, jobErr, "active") })
}

// guard runs one listener callback. A panic is recovered and reported to OnError
// rather than taking the process down: a handler is the consumer's code doing side
// effects, and this port has no business crashing a worker over it.
func (w *Worker) guard(ctx context.Context, job *Job, event string, call func()) {
	defer func() {
		if r := recover(); r != nil {
			w.reportError(ctx, job, fmt.Errorf("bullmq: %s listener panicked: %v", event, r))
		}
	}()
	call()
}

// reportError hands an error of the worker itself to OnError. A cancelled context
// is the shutdown itself rather than a fault, so it is not reported — upstream
// suppresses the same way while closing (worker.ts:1592).
//
// This is the one callback whose panic is swallowed: routing it back into OnError
// would loop, and there is nowhere else left to report it.
func (w *Worker) reportError(ctx context.Context, job *Job, err error) {
	if w.listener.OnError == nil || err == nil {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	defer func() { _ = recover() }()
	w.listener.OnError(ctx, job, err)
}

func (w *Worker) registerActive(jobID, token string) {
	w.mu.Lock()
	w.active[jobID] = token
	w.mu.Unlock()
}

// unregisterActive drops a job from the lock-renewal set, but only while the entry
// is still the one this run put there. The map is keyed by job id, and a retried
// job can be picked up again — by this very worker — before the previous run has
// finished reporting it, in which case the entry already belongs to the new run and
// deleting it would leave that run without lock renewal.
func (w *Worker) unregisterActive(jobID, token string) {
	w.mu.Lock()
	if w.active[jobID] == token {
		delete(w.active, jobID)
	}
	w.mu.Unlock()
}

// lockRenewalLoop renews the locks of all in-flight jobs every lockDuration/2, so
// long-running jobs are not mistaken for stalled. Mirrors python Worker.extendLocks.
func (w *Worker) lockRenewalLoop(ctx context.Context) {
	interval := time.Duration(w.lockDuration/2) * time.Millisecond
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.extendLocks(ctx)
		}
	}
}

func (w *Worker) extendLocks(ctx context.Context) {
	w.mu.Lock()
	snapshot := make(map[string]string, len(w.active))
	for id, token := range w.active {
		snapshot[id] = token
	}
	w.mu.Unlock()
	for id, token := range snapshot {
		if _, err := w.queue.scripts.extendLock(ctx, id, token, w.lockDuration); err != nil {
			// Upstream reports a failed renewal too (worker.ts emits 'error' via
			// retryIfFailed, plus a 'lockRenewalFailed' event carrying the ids).
			// A lost lock means the job is about to be run twice, so it must not
			// stay silent.
			w.reportError(ctx, nil, fmt.Errorf("bullmq: renew lock of job %s: %w", id, err))
		}
	}
}

// stalledCheckLoop periodically moves jobs whose worker died back to wait.
func (w *Worker) stalledCheckLoop(ctx context.Context) {
	interval := time.Duration(w.stalledInterval) * time.Millisecond
	if interval <= 0 {
		interval = defaultDrainDelay
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := w.queue.scripts.moveStalledJobsToWait(ctx, w.maxStalledCount, w.stalledInterval); err != nil {
				w.reportError(ctx, nil, fmt.Errorf("bullmq: stalled check: %w", err))
			}
		}
	}
}

// ensureClientName sets a CLIENT SETNAME on the worker's connections so
// Queue.GetWorkers can find it via CLIENT LIST. Best-effort.
func (w *Worker) ensureClientName(ctx context.Context) {
	suffix := ""
	if w.workerName != "" {
		suffix = ":w:" + w.workerName
	}
	name := w.queue.keys.ClientName(suffix)
	_ = w.queue.conn.blockingClient.Do(ctx, "CLIENT", "SETNAME", name).Err()
	_ = w.queue.conn.client.Do(ctx, "CLIENT", "SETNAME", name).Err()
}

// Close stops fetching new jobs, waits for the ones already being processed to
// finish, and only then releases the clients this worker owns (an injected client
// is left to its owner). Run returns nil once the drain completes; a cancelled
// caller context is still reported as such.
//
// Jobs in flight keep the context they were given, so they are awaited rather than
// aborted — the same as upstream's non-forced close — and their locks keep being
// renewed for the whole drain, so a job slower than lockDuration is not mistaken
// for stalled and picked up by another worker. A processor that neither
// returns nor honours its context therefore holds Close for as long as it runs;
// cancel the context passed to Run to abort such work instead. For the same reason
// Close must not be called from inside a processor: it would wait for the very job
// making the call.
//
// It is safe to call more than once — Close is often reached from both a defer and
// a cleanup — and every call reports the result of the first.
func (w *Worker) Close() error {
	w.closeOnce.Do(func() {
		close(w.closed)
		// Drop the blocking peer first: it is parked on the marker for up to the
		// drain delay, and cancelling a context does not interrupt a read already in
		// flight. Jobs still running keep using the main client.
		_ = w.queue.conn.closeBlocking()
		if w.running.Load() {
			<-w.runDone // jobs in flight finish against live clients
		}
		w.closeErr = w.queue.Close()
	})
	return w.closeErr
}

// genID returns a random hex id used to namespace lock tokens.
func genID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
