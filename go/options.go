package bullmq

import "github.com/redis/go-redis/v9"

// config holds the settings shared by Queue, Worker and FlowProducer. It is
// populated through functional Options. Worker-only fields are ignored by Queue.
type config struct {
	prefix         string
	client         redis.UniversalClient
	blockingClient redis.UniversalClient
	redisOptions   *redis.Options

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
}

// Limiter rate-limits a worker to Max jobs per Duration (milliseconds).
type Limiter struct {
	Max      int
	Duration int64
}

// Option customises a Queue/Worker/FlowProducer at construction time.
// The functional-options pattern keeps the API DI-friendly (see PLAN §3).
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

// WithMetrics makes the worker record completed/failed job counts as a time series
// (capped at maxDataPoints), readable via Queue.GetMetrics.
func WithMetrics(maxDataPoints int) Option {
	return func(cfg *config) { cfg.metricsMaxDataPoints = maxDataPoints }
}

// newConfig applies options over the defaults.
func newConfig(opts ...Option) *config {
	cfg := &config{prefix: DefaultPrefix}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}
