package bullmq

import (
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestOptionsApply(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	cfg := newConfig(
		WithPrefix("myapp"),
		WithClient(client),
	)
	if cfg.prefix != "myapp" {
		t.Errorf("prefix = %q, want %q", cfg.prefix, "myapp")
	}
	if cfg.client != client {
		t.Error("WithClient did not set the injected client")
	}
}

func TestOptionsDefaultPrefix(t *testing.T) {
	cfg := newConfig(WithRedisOptions(&redis.Options{Addr: "127.0.0.1:6379"}))
	if cfg.prefix != DefaultPrefix {
		t.Errorf("default prefix = %q, want %q", cfg.prefix, DefaultPrefix)
	}
	if cfg.redisOptions == nil {
		t.Error("WithRedisOptions did not set redisOptions")
	}
}
