package bullmq

import (
	"testing"

	"github.com/redis/go-redis/v9"
)

// duplicate mirrors ioredis client.duplicate(): a fresh client with the same options.
func TestDuplicateClient(t *testing.T) {
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 3})
	d, err := duplicate(c)
	if err != nil {
		t.Fatalf("duplicate: %v", err)
	}
	dc, ok := d.(*redis.Client)
	if !ok {
		t.Fatalf("duplicate returned %T, want *redis.Client", d)
	}
	if dc == c {
		t.Error("duplicate returned the same instance, want a distinct one")
	}
	if got := dc.Options().Addr; got != "127.0.0.1:6379" {
		t.Errorf("duplicate Addr = %q, want %q", got, "127.0.0.1:6379")
	}
	if got := dc.Options().DB; got != 3 {
		t.Errorf("duplicate DB = %d, want 3", got)
	}
}

// An injected client is not owned (Close must not touch it); its blocking peer is
// derived by duplication and owned by us.
func TestNewConnectionInjectedClient(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	conn, err := newConnection(WithClient(client))
	if err != nil {
		t.Fatalf("newConnection: %v", err)
	}
	if conn.client != client {
		t.Error("connection did not use the injected client")
	}
	if conn.ownsClient {
		t.Error("ownsClient must be false for an injected client")
	}
	if conn.blockingClient == nil || conn.blockingClient == client {
		t.Error("blockingClient must be a distinct duplicate")
	}
	if len(conn.scripts) == 0 {
		t.Error("script registry is empty")
	}
}

// With only options, the library builds and owns the client.
func TestNewConnectionBuiltClient(t *testing.T) {
	conn, err := newConnection(WithRedisOptions(&redis.Options{Addr: "127.0.0.1:6379"}))
	if err != nil {
		t.Fatalf("newConnection: %v", err)
	}
	if conn.client == nil {
		t.Fatal("client was not built from options")
	}
	if !conn.ownsClient {
		t.Error("ownsClient must be true for a library-built client")
	}
}

func TestNewConnectionRequiresClientOrOptions(t *testing.T) {
	if _, err := newConnection(); err == nil {
		t.Error("newConnection with neither client nor options should error")
	}
}
