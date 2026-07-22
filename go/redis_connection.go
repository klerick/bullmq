package bullmq

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// connection wires a Redis client (and a dedicated blocking peer) to the registry
// of embedded Lua scripts. Ported from python/bullmq/redis_connection.py, extended
// with client injection for DI.
type connection struct {
	client         redis.UniversalClient
	blockingClient redis.UniversalClient
	scripts        map[string]*redis.Script

	// Ownership: a client we built ourselves is closed on Close; an injected one
	// is left alone so it keeps serving the DI container that created it.
	ownsClient   bool
	ownsBlocking bool
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

	scripts, err := loadScriptRegistry()
	if err != nil {
		return nil, err
	}
	conn.scripts = scripts

	return conn, nil
}

// loadScriptRegistry turns every embedded Lua command into a *redis.Script.
// redis.NewScript caches the SHA and transparently falls back EVALSHA -> EVAL on
// NOSCRIPT, so a manual EVALSHA loop (as in rust) is unnecessary.
func loadScriptRegistry() (map[string]*redis.Script, error) {
	raw, err := loadRawScripts()
	if err != nil {
		return nil, err
	}
	scripts := make(map[string]*redis.Script, len(raw))
	for name, s := range raw {
		scripts[name] = redis.NewScript(s.content)
	}
	return scripts, nil
}

// LoadScripts pre-loads every script into Redis with SCRIPT LOAD. This is required
// before running scripts inside a pipeline/MULTI, where the NOSCRIPT fallback is
// not available (mirrors rust load_all / python register_script).
func (c *connection) LoadScripts(ctx context.Context) error {
	for name, script := range c.scripts {
		if err := script.Load(ctx, c.client).Err(); err != nil {
			return fmt.Errorf("bullmq: loading script %q: %w", name, err)
		}
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
