package bullmq

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// testRedisOptions points integration tests at a real Redis (docker-compose or CI
// service). A dedicated DB keeps test keys away from anything else.
func testRedisOptions() *redis.Options {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	return &redis.Options{Addr: addr, DB: 15}
}

// requireRedis returns a client to a live Redis, or skips the test if none is
// reachable — so `go test` stays green on machines without Redis while still
// exercising real scripts where Redis is available. BullMQ's Lua needs cmsgpack/
// cjson, which only a real Redis provides (miniredis cannot run these scripts).
func requireRedis(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(testRedisOptions())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Skipf("redis not available (%v); skipping integration test", err)
	}
	return client
}

// LoadScripts must push every embedded command into Redis (SCRIPT LOAD), so later
// EVALSHA calls inside pipelines succeed. Verifies the embed -> registry -> Redis
// pipeline end to end against a real server.
func TestLoadScriptsIntegration(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()

	// Start from a clean script cache so SCRIPT EXISTS reflects only our load.
	if err := client.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("script flush: %v", err)
	}

	conn, err := newConnection(WithClient(client))
	if err != nil {
		t.Fatalf("newConnection: %v", err)
	}
	if err := conn.LoadScripts(ctx); err != nil {
		t.Fatalf("LoadScripts: %v", err)
	}

	hashes := make([]string, 0, len(conn.scripts))
	for _, s := range conn.scripts {
		hashes = append(hashes, s.Hash())
	}
	exists, err := client.ScriptExists(ctx, hashes...).Result()
	if err != nil {
		t.Fatalf("SCRIPT EXISTS: %v", err)
	}
	for i, ok := range exists {
		if !ok {
			t.Errorf("script %s not loaded", hashes[i])
		}
	}
}
