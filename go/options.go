package bullmq

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// config holds the settings shared by Queue, Worker and FlowProducer. It is
// populated through functional Options. Worker-only fields are ignored by Queue.
type config struct {
	prefix          string
	client          redis.UniversalClient
	blockingClient  redis.UniversalClient
	redisOptions    *redis.Options
	telemetry       Telemetry
	backoffStrategy BackoffStrategy

	// worker-only
	concurrency          int
	lockDuration         int64 // milliseconds
	stalledInterval      int64 // milliseconds
	maxStalledCount      int
	limiter              *Limiter
	workerName           string
	skipStalledCheck     bool
	skipLockRenewal      bool
	metricsMaxDataPoints int
	listener             WorkerListener
}

// WorkerListener holds the callbacks a Worker invokes as its jobs move through
// their lifecycle. Every field is optional. Named after upstream's WorkerListener
// (src/classes/worker.ts:59), which carries the same events with the same arguments.
//
// A callback runs synchronously, in the goroutine of the job it belongs to, right
// after the state change it reports. That is not a divergence: Node's
// EventEmitter.emit calls its listeners inline, and so does python's
// (python/bullmq/event_emitter.py). It is also what makes these callbacks usable
// for side effects — Close drains the jobs in flight, so a handler is awaited
// rather than cut off mid-way by a rollout. The flip side is that a slow handler
// holds its concurrency slot and Close for as long as it runs, exactly like a slow
// processor.
//
// Unlike QueueEvents, which reads a Redis stream, these fire in the process that
// finalised the job: nothing is lost across a restart and nothing is delivered
// twice when several workers share a queue.
//
// The context is the job's process context, so a handler can open a span nesting
// under the job's 'process' span.
//
// A panic in a callback is recovered and reported to OnError instead of taking the
// process down with it.
type WorkerListener struct {
	// OnActive reports a job entering the active state. prev is "waiting".
	OnActive func(ctx context.Context, job *Job, prev string)

	// OnCompleted reports a job that succeeded, with the processor's return value.
	// prev is "active".
	OnCompleted func(ctx context.Context, job *Job, result any, prev string)

	// OnFailed reports an attempt that ended in an error — not necessarily the
	// last one. It fires on every failed attempt, as upstream does, and
	// Job.FinishedOn tells the two apart: zero while retries remain, set once the
	// job has failed for good. prev is "active".
	//
	// It also fires for a parent failed by the failParentOnFailure cascade, whose
	// processor never ran: err is then an *UnrecoverableError carrying the reason
	// recorded by Lua.
	OnFailed func(ctx context.Context, job *Job, err error, prev string)

	// OnError reports an error of the worker itself rather than an outcome of a
	// job: a finalisation that could not be written to Redis, a lock renewal or
	// stalled check that failed, or a panic recovered from one of the callbacks
	// above. job is nil when the error belongs to no particular job. Upstream's
	// equivalent is emit('error') (worker.ts:1593).
	OnError func(ctx context.Context, job *Job, err error)
}

// Limiter rate-limits a worker to Max jobs per Duration (milliseconds).
type Limiter struct {
	Max      int
	Duration int64
}

// Option customises a Queue/Worker/FlowProducer at construction time.
// The functional-options pattern keeps the API DI-friendly.
type Option func(*config)

// WithClient injects an already-built Redis client. This is the primary path for
// zest's DI: the client is owned by the caller and left open on Close.
func WithClient(c redis.UniversalClient) Option {
	return func(cfg *config) { cfg.client = c }
}

// WithBlockingClient injects a dedicated client for blocking commands (BZPOPMIN).
// If omitted, one is derived from the main client via duplicate(). This is an
// escape hatch for UniversalClient implementations that duplicate() cannot clone.
func WithBlockingClient(c redis.UniversalClient) Option {
	return func(cfg *config) { cfg.blockingClient = c }
}

// WithRedisOptions builds the client from options instead of injecting one. The
// library then owns the client and closes it on Close.
func WithRedisOptions(o *redis.Options) Option {
	return func(cfg *config) { cfg.redisOptions = o }
}

// WithPrefix overrides the key prefix (default "bull").
func WithPrefix(prefix string) Option {
	return func(cfg *config) { cfg.prefix = prefix }
}

// WithTelemetry enables distributed tracing via the given Telemetry implementation
// (e.g. bullmq/otel). Trace context propagates through jobs so add and process
// spans link across processes and runtimes.
func WithTelemetry(t Telemetry) Option {
	return func(cfg *config) { cfg.telemetry = t }
}

// WithConcurrency sets how many jobs a Worker processes in parallel (default 1).
func WithConcurrency(n int) Option {
	return func(cfg *config) { cfg.concurrency = n }
}

// WithLockDuration sets the job lock duration in milliseconds (default 30000).
func WithLockDuration(ms int64) Option {
	return func(cfg *config) { cfg.lockDuration = ms }
}

// WithStalledInterval sets how often (ms) the worker checks for stalled jobs
// (default 30000).
func WithStalledInterval(ms int64) Option {
	return func(cfg *config) { cfg.stalledInterval = ms }
}

// WithMaxStalledCount sets how many times a job may be recovered from the stalled
// state before being failed (default 1).
func WithMaxStalledCount(n int) Option {
	return func(cfg *config) { cfg.maxStalledCount = n }
}

// WithLimiter rate-limits the worker to max jobs per durationMs milliseconds.
func WithLimiter(max int, durationMs int64) Option {
	return func(cfg *config) { cfg.limiter = &Limiter{Max: max, Duration: durationMs} }
}

// WithWorkerName names the worker so Queue.GetWorkers can identify it.
func WithWorkerName(name string) Option {
	return func(cfg *config) { cfg.workerName = name }
}

// WithSkipStalledCheck disables the worker's stalled-job recovery loop.
func WithSkipStalledCheck() Option {
	return func(cfg *config) { cfg.skipStalledCheck = true }
}

// WithSkipLockRenewal disables the worker's lock-renewal loop.
func WithSkipLockRenewal() Option {
	return func(cfg *config) { cfg.skipLockRenewal = true }
}

// WithWorkerListener registers lifecycle callbacks on a Worker. Set at
// construction, so no listener can be added to a running worker and none needs a
// lock. See WorkerListener for when each callback runs.
func WithWorkerListener(l WorkerListener) Option {
	return func(cfg *config) { cfg.listener = l }
}

// WithMetrics makes the worker record completed/failed job counts as a time series
// (capped at maxDataPoints), readable via Queue.GetMetrics.
func WithMetrics(maxDataPoints int) Option {
	return func(cfg *config) { cfg.metricsMaxDataPoints = maxDataPoints }
}

// WithBackoffStrategy registers a custom backoff strategy used for jobs whose
// backoff type is not "fixed" or "exponential".
func WithBackoffStrategy(fn BackoffStrategy) Option {
	return func(cfg *config) { cfg.backoffStrategy = fn }
}

// newConfig applies options over the defaults.
func newConfig(opts ...Option) *config {
	cfg := &config{prefix: DefaultPrefix}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}
