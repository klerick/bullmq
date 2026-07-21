package bullmq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"sync"
	"time"
)

const (
	defaultLockDuration    int64 = 30000 // ms
	defaultStalledInterval int64 = 30000 // ms
	defaultMaxStalledCount       = 1
	defaultDrainDelay            = 5 * time.Second
	maxBlockTimeout              = 10 * time.Second
)

// Processor handles a single job. Returning a value completes the job; returning
// an error fails it (subject to the retry policy). Return ErrWaitingChildren to
// leave the job in the waiting-children state without finalising it.
type Processor func(ctx context.Context, job *Job) (any, error)

// Worker fetches jobs from a queue and runs them through a Processor. Construct it
// with NewWorker and the same functional options as Queue, plus WithConcurrency /
// WithLockDuration.
type Worker struct {
	queue            *Queue
	processor        Processor
	concurrency      int
	lockDuration     int64
	stalledInterval  int64
	maxStalledCount  int
	limiter          *Limiter
	workerName       string
	skipStalledCheck bool
	skipLockRenewal  bool
	drainDelay       time.Duration
	id               string

	drained    bool
	blockUntil int64

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
	maxStalledCount := cfg.maxStalledCount
	if maxStalledCount == 0 {
		maxStalledCount = defaultMaxStalledCount
	}
	return &Worker{
		queue:            q,
		processor:        processor,
		concurrency:      concurrency,
		lockDuration:     lockDuration,
		stalledInterval:  stalledInterval,
		maxStalledCount:  maxStalledCount,
		limiter:          cfg.limiter,
		workerName:       cfg.workerName,
		skipStalledCheck: cfg.skipStalledCheck,
		skipLockRenewal:  cfg.skipLockRenewal,
		drainDelay:       defaultDrainDelay,
		id:               genID(),
		drained:          true,
		active:           make(map[string]string),
	}, nil
}

// Run processes jobs until ctx is cancelled. It blocks, so callers typically run
// it in a goroutine. In-flight jobs are awaited before Run returns.
func (w *Worker) Run(ctx context.Context) error {
	// Register a client name so Queue.GetWorkers can discover this worker
	// (best-effort; some managed Redis providers reject CLIENT SETNAME).
	w.ensureClientName(ctx)

	sem := make(chan struct{}, w.concurrency)
	var wg sync.WaitGroup

	// Background maintenance: renew locks of active jobs, and recover stalled ones.
	if !w.skipLockRenewal {
		wg.Add(1)
		go func() { defer wg.Done(); w.lockRenewalLoop(ctx) }()
	}
	if !w.skipStalledCheck {
		wg.Add(1)
		go func() { defer wg.Done(); w.stalledCheckLoop(ctx) }()
	}

	seq := 0

	for ctx.Err() == nil {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		}

		seq++
		token := w.id + ":" + strconv.Itoa(seq)
		job, err := w.getNextJob(ctx, token)
		if err != nil {
			<-sem
			if ctx.Err() != nil {
				break
			}
			time.Sleep(100 * time.Millisecond) // bounded retry on transient error
			continue
		}
		if job == nil {
			<-sem
			continue
		}

		wg.Add(1)
		go func(job *Job) {
			defer wg.Done()
			defer func() { <-sem }()
			w.processJob(ctx, job)
		}(job)
	}

	wg.Wait()
	return ctx.Err()
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
func (w *Worker) waitForJob(ctx context.Context) {
	timeout := w.blockTimeout()
	_, _ = w.queue.conn.blockingClient.BZPopMin(ctx, timeout, w.queue.keys.Marker()).Result()
	w.blockUntil = 0
}

func (w *Worker) blockTimeout() time.Duration {
	if w.blockUntil > 0 {
		d := w.blockUntil - nowMillis()
		if d <= 0 {
			return time.Millisecond
		}
		if dur := time.Duration(d) * time.Millisecond; dur < maxBlockTimeout {
			return dur
		}
		return maxBlockTimeout
	}
	return w.drainDelay
}

func (w *Worker) processJob(ctx context.Context, job *Job) {
	w.registerActive(job.ID, job.token)
	defer w.unregisterActive(job.ID)

	fo := finishOpts{
		token:            job.token,
		lockDuration:     w.lockDuration,
		removeOnComplete: job.opts["removeOnComplete"],
		removeOnFail:     job.opts["removeOnFail"],
	}
	result, procErr := w.processor(ctx, job)
	if procErr != nil {
		if errors.Is(procErr, ErrWaitingChildren) {
			return // job intentionally left in waiting-children
		}
		_ = job.moveToFailed(ctx, procErr, fo, false)
		return
	}
	_ = job.moveToCompleted(ctx, result, fo, false)
}

func (w *Worker) registerActive(jobID, token string) {
	w.mu.Lock()
	w.active[jobID] = token
	w.mu.Unlock()
}

func (w *Worker) unregisterActive(jobID string) {
	w.mu.Lock()
	delete(w.active, jobID)
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
		_, _ = w.queue.scripts.extendLock(ctx, id, token, w.lockDuration)
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
			_, _ = w.queue.scripts.moveStalledJobsToWait(ctx, w.maxStalledCount, w.stalledInterval)
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

// Close stops using resources, closing only clients the worker owns.
func (w *Worker) Close() error { return w.queue.Close() }

// genID returns a random hex id used to namespace lock tokens.
func genID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
