package bullmq

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
)

// Queue.Add must write a job hash in the exact schema Node's Job.fromJSON reads:
// name, data (our JSON verbatim), opts (cjson), timestamp, delay, priority. Storing
// this correctly is the interop guarantee — a Node consumer reads these same fields.
// See storeJob.lua.
func TestQueueAddStoresInteropHash(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("go-interop", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}

	job, err := q.Add(ctx, "createUser",
		map[string]any{"email": "a@b.c", "n": 3},
		&JobOptions{Attempts: 2})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	// First job in a flushed queue gets the Lua-generated id "1".
	if job.ID != "1" {
		t.Errorf("generated job id = %q, want %q", job.ID, "1")
	}

	hash, err := client.HGetAll(ctx, q.keys.JobKey(job.ID)).Result()
	if err != nil {
		t.Fatalf("HGetAll: %v", err)
	}

	if hash["name"] != "createUser" {
		t.Errorf("hash name = %q, want %q", hash["name"], "createUser")
	}
	// Data is stored verbatim; Go marshals map keys sorted -> deterministic.
	if hash["data"] != `{"email":"a@b.c","n":3}` {
		t.Errorf("hash data = %q, want %q", hash["data"], `{"email":"a@b.c","n":3}`)
	}
	if hash["timestamp"] != strconv.FormatInt(job.Timestamp, 10) {
		t.Errorf("hash timestamp = %q, want %q", hash["timestamp"], strconv.FormatInt(job.Timestamp, 10))
	}
	if hash["delay"] != "0" {
		t.Errorf("hash delay = %q, want %q", hash["delay"], "0")
	}

	// opts is re-encoded by cjson (key order not guaranteed) — parse and check.
	var opts map[string]any
	if err := json.Unmarshal([]byte(hash["opts"]), &opts); err != nil {
		t.Fatalf("opts is not valid JSON: %v (%q)", err, hash["opts"])
	}
	if opts["attempts"] != float64(2) {
		t.Errorf("opts attempts = %v, want 2", opts["attempts"])
	}
}

// A delayed job dispatches to addDelayedJob and lands in the delayed zset.
func TestQueueAddDelayed(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, err := NewQueue("go-delayed", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	job, err := q.Add(ctx, "later", map[string]any{"x": 1}, &JobOptions{Delay: 10000})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if n := client.ZCard(ctx, q.keys.Delayed()).Val(); n != 1 {
		t.Errorf("delayed zset card = %d, want 1", n)
	}
	if d := client.HGet(ctx, q.keys.JobKey(job.ID), "delay").Val(); d != "10000" {
		t.Errorf("hash delay = %q, want %q", d, "10000")
	}
}

// A custom job id is returned as-is (string path in parseAddResult).
func TestQueueAddCustomID(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, err := NewQueue("go-customid", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	job, err := q.Add(ctx, "n", nil, &JobOptions{JobID: "my-custom"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if job.ID != "my-custom" {
		t.Errorf("job id = %q, want %q", job.ID, "my-custom")
	}
	if client.Exists(ctx, q.keys.JobKey("my-custom")).Val() != 1 {
		t.Error("job hash for custom id not found")
	}
}
