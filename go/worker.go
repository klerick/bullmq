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
	defaultLockDuration int64 = 30000 // ms
	defaultDrainDelay         = 5 * time.Second
	maxBlockTimeout           = 10 * time.Second
)

// Processor handles a single job. Returning a value completes the job; returning
// an error fails it (subject to the retry policy). Return ErrWaitingChildren to
// leave the job in the waiting-children state without finalising it.
type Processor func(ctx context.Context, job *Job) (any, error)

// Worker fetches jobs from a queue and runs them through a Processor. Construct it
// with NewWorker and the same functional options as Queue, plus WithConcurrency /
// WithLockDuration.
type Worker struct {
	queue        *Queue
	processor    Processor
	concurrency  int
	lockDuration int64
	drainDelay   time.Duration
	id           string

	drained    bool
	blockUntil int64
	limitUntil int64
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
	return &Worker{
		queue:        q,
		processor:    processor,
		concurrency:  concurrency,
		lockDuration: lockDuration,
		drainDelay:   defaultDrainDelay,
		id:           genID(),
		drained:      true,
	}, nil
}

// Run processes jobs until ctx is cancelled. It blocks, so callers typically run
// it in a goroutine. In-flight jobs are awaited before Run returns.
func (w *Worker) Run(ctx context.Context) error {
	sem := make(chan struct{}, w.concurrency)
	var wg sync.WaitGroup
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
	jobData, jobID, limitUntil, delayUntil, err := w.queue.scripts.moveToActive(ctx, moveToActiveOpts{
		token:        token,
		lockDuration: w.lockDuration,
	})
	if err != nil {
		return nil, err
	}
	if limitUntil > 0 {
		w.limitUntil = limitUntil
	}
	if jobData == nil {
		if delayUntil > 0 {
			w.blockUntil = delayUntil
		}
		return nil, nil
	}
	job := jobFromRaw(w.queue, jobData, jobID)
	job.token = token
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

// Close stops using resources, closing only clients the worker owns.
func (w *Worker) Close() error { return w.queue.Close() }

// genID returns a random hex id used to namespace lock tokens.
func genID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
