package bullmq

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

// connection wires a Redis client (and a dedicated blocking peer) to the registry
// of embedded Lua scripts. Ported from python/bullmq/redis_connection.py, extended
// with client injection for DI.
type connection struct {
	client         redis.UniversalClient
	blockingClient redis.UniversalClient
	scripts        map[string]*redis.Script
	scriptSrc      map[string]string // Lua source per script, for pipelined SCRIPT LOAD

	// scriptCache tracks whether those scripts are currently loaded in Redis.
	scriptCache scriptCache

	// Ownership: a client we built ourselves is closed on Close; an injected one
	// is left alone so it keeps serving the DI container that created it.
	ownsClient   bool
	ownsBlocking bool
}

// scriptCache remembers whether the Lua scripts are loaded in Redis. "Loaded" is
// a fact with an expiry date — a restart, a failover or SCRIPT FLUSH empties the
// server-side cache — so the state must be resettable, unlike the sync.Once it
// replaces. The generation counter is what makes recovery single-flight: callers
// that hit NOSCRIPT ask for a reload of the generation they ran against, and
// however many ask at once, only the first one actually re-sends the scripts.
type scriptCache struct {
	mu   sync.Mutex
	gen  uint64                      // 0 = not loaded; +1 per successful load
	load func(context.Context) error // set by newConnectionFromConfig
}

// ensure loads the scripts unless a previous load is still believed good, and
// returns the generation the caller is about to run against.
func (s *scriptCache) ensure(ctx context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gen != 0 {
		return s.gen, nil
	}
	if err := s.load(ctx); err != nil {
		return 0, err // transient: the next call retries instead of caching the failure
	}
	s.gen++
	return s.gen, nil
}

// reload re-sends the scripts after a NOSCRIPT, unless someone else already did it
// for this generation. Holding the lock across the load is the single flight:
// latecomers wake to a newer generation and skip their own sweep.
func (s *scriptCache) reload(ctx context.Context, seen uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gen != seen {
		return nil
	}
	if err := s.load(ctx); err != nil {
		s.gen = 0 // what Redis holds is unknown now; the next call loads from scratch
		return err
	}
	s.gen++
	return nil
}

// generation reports how many successful loads this cache has done.
func (s *scriptCache) generation() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gen
}

// newConnection resolves the client (injected or built from options), derives a
// blocking peer, and registers every embedded Lua script.
func newConnection(opts ...Option) (*connection, error) {
	cfg := newConfig(opts...)
	return newConnectionFromConfig(cfg)
}

func newConnectionFromConfig(cfg *config) (*connection, error) {
	conn := &connection{}

	switch {
	case cfg.client != nil:
		conn.client = cfg.client
		conn.ownsClient = false
	case cfg.redisOptions != nil:
		conn.client = redis.NewClient(cfg.redisOptions)
		conn.ownsClient = true
	default:
		return nil, fmt.Errorf("%w: a Redis client (WithClient) or options (WithRedisOptions) is required", ErrInvalidConfig)
	}

	// A blocking command (BZPOPMIN on the marker) holds a connection for the whole
	// block timeout, so it must not share the main pool. Prefer an explicit blocking
	// client; otherwise duplicate the main one (mirrors ioredis client.duplicate()).
	if cfg.blockingClient != nil {
		conn.blockingClient = cfg.blockingClient
		conn.ownsBlocking = false
	} else {
		dup, err := duplicate(conn.client)
		if err != nil {
			return nil, err
		}
		conn.blockingClient = dup
		conn.ownsBlocking = true
	}

	scripts, sources, err := loadScriptRegistry()
	if err != nil {
		return nil, err
	}
	conn.scripts = scripts
	conn.scriptSrc = sources
	conn.scriptCache.load = conn.sendScripts

	return conn, nil
}

// loadScriptRegistry turns every embedded Lua command into a *redis.Script, and
// returns the sources alongside for the pipelined preload. redis.NewScript caches
// the SHA and transparently falls back EVALSHA -> EVAL on NOSCRIPT, so a manual
// EVALSHA loop (as in rust) is unnecessary outside pipelines.
func loadScriptRegistry() (map[string]*redis.Script, map[string]string, error) {
	raw, err := loadRawScripts()
	if err != nil {
		return nil, nil, err
	}
	scripts := make(map[string]*redis.Script, len(raw))
	sources := make(map[string]string, len(raw))
	for name, s := range raw {
		scripts[name] = redis.NewScript(s.content)
		sources[name] = s.content
	}
	return scripts, sources, nil
}

// LoadScripts makes sure every script is loaded into Redis (SCRIPT LOAD). This is
// required before running scripts inside a pipeline/MULTI, where the NOSCRIPT
// fallback is not available (mirrors rust load_all / python register_script).
// Repeat calls are free while the cache is believed good.
func (c *connection) LoadScripts(ctx context.Context) error {
	_, err := c.scriptCache.ensure(ctx)
	return err
}

// sendScripts SCRIPT LOADs the whole registry in one round trip. Sending them one
// by one costs 49 round trips, which is paid again on every reload and would widen
// the window in which a concurrent pipeline sees a half-loaded cache.
//
// It sends the raw sources rather than calling redis.Script.Load with the pipeline:
// that helper assigns s.hash = cmd.Val() right after queueing the command, and a
// queued command has no value yet, so it would blank out the hash every script is
// later EVALSHA'd by — turning the preload into a guaranteed NOSCRIPT.
func (c *connection) sendScripts(ctx context.Context) error {
	cmds := make(map[string]*redis.StringCmd, len(c.scriptSrc))
	_, pipeErr := c.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for name, src := range c.scriptSrc {
			cmds[name] = pipe.ScriptLoad(ctx, src)
		}
		return nil
	})
	// Per-command errors first: they name the script that failed.
	for name, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			return fmt.Errorf("bullmq: loading script %q: %w", name, err)
		}
	}
	if pipeErr != nil {
		return fmt.Errorf("bullmq: loading scripts: %w", pipeErr)
	}
	return nil
}

// Close closes only the clients this connection owns. Injected clients are left
// open for their owner (the DI container).
func (c *connection) Close() error {
	var firstErr error
	if c.ownsBlocking && c.blockingClient != nil {
		if err := c.blockingClient.Close(); err != nil {
			firstErr = err
		}
	}
	if c.ownsClient && c.client != nil {
		if err := c.client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// duplicate returns a fresh client with the same options as c, mirroring ioredis
// client.duplicate(). go-redis has no generic Options() on UniversalClient, so we
// switch on the concrete type.
func duplicate(c redis.UniversalClient) (redis.UniversalClient, error) {
	switch cc := c.(type) {
	case *redis.Client:
		return redis.NewClient(cc.Options()), nil
	case *redis.ClusterClient:
		return redis.NewClusterClient(cc.Options()), nil
	case *redis.Ring:
		return redis.NewRing(cc.Options()), nil
	default:
		return nil, fmt.Errorf("%w: cannot duplicate client of type %T; pass WithBlockingClient", ErrInvalidConfig, c)
	}
}
