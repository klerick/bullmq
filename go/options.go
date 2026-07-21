package bullmq

import "github.com/redis/go-redis/v9"

// config holds the connection-level settings shared by Queue, Worker and
// FlowProducer. It is populated through functional Options.
type config struct {
	prefix         string
	client         redis.UniversalClient
	blockingClient redis.UniversalClient
	redisOptions   *redis.Options
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

// newConfig applies options over the defaults.
func newConfig(opts ...Option) *config {
	cfg := &config{prefix: DefaultPrefix}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}
