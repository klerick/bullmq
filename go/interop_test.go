package bullmq

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// requireNodeInterop skips the test unless Node and the pinned bullmq interop
// harness are installed (go/interop/node_modules). CI installs them; locally run
// `npm install` in go/interop to enable these tests.
func requireNodeInterop(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not found; skipping cross-runtime interop test")
	}
	if _, err := os.Stat("interop/node_modules/bullmq"); err != nil {
		t.Skip("go/interop deps not installed (run `npm install` in go/interop); skipping interop test")
	}
}

// runNode executes an interop harness script and returns its stdout, pointing it
// at the same Redis host/DB the Go tests use.
func runNode(t *testing.T, script string, args ...string) string {
	t.Helper()
	host, port := "127.0.0.1", "6379"
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		if h, p, ok := strings.Cut(addr, ":"); ok {
			host, port = h, p
		}
	}
	cmd := exec.Command("node", append([]string{"interop/" + script}, args...)...)
	cmd.Env = append(os.Environ(),
		"REDIS_HOST="+host,
		"REDIS_PORT="+port,
		"REDIS_DB=15", // matches testRedisOptions
	)
	out, err := cmd.Output()
	if err != nil {
		stderr := ""
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = string(ee.Stderr)
		}
		t.Fatalf("node %s %v failed: %v\n%s", script, args, err, stderr)
	}
	return string(out)
}

// Go produces a job; Node's BullMQ reads it back with identical name/data/opts.
func TestInteropGoAddNodeReads(t *testing.T) {
	client := requireRedis(t)
	requireNodeInterop(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	q, err := NewQueue("interop-a", WithClient(client))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()
	job, err := q.Add(ctx, "createUser",
		map[string]any{"email": "x@y.z", "n": 7},
		&JobOptions{Attempts: 2})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	out := runNode(t, "read.mjs", "bull", "interop-a", job.ID)
	var got struct {
		Name string         `json:"name"`
		Data map[string]any `json:"data"`
		Opts map[string]any `json:"opts"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("cannot parse node output %q: %v", out, err)
	}
	if got.Name != "createUser" {
		t.Errorf("Node read name = %q, want %q", got.Name, "createUser")
	}
	if got.Data["email"] != "x@y.z" || got.Data["n"] != float64(7) {
		t.Errorf("Node read data = %v, want {email:x@y.z, n:7}", got.Data)
	}
	if got.Opts["attempts"] != float64(2) {
		t.Errorf("Node read opts.attempts = %v, want 2", got.Opts["attempts"])
	}
}

// Node produces a job; a Go worker consumes it and completes it.
func TestInteropNodeAddGoProcesses(t *testing.T) {
	client := requireRedis(t)
	requireNodeInterop(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	jobID := strings.TrimSpace(runNode(t, "add.mjs", "bull", "interop-b", "greet", `{"hello":"world"}`))
	if jobID == "" {
		t.Fatal("node add.mjs returned an empty job id")
	}

	got := make(chan any, 1)
	w, err := NewWorker("interop-b", func(ctx context.Context, j *Job) (any, error) {
		got <- j.Data
		return "processed-by-go", nil
	}, WithClient(client))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	select {
	case data := <-got:
		m, ok := data.(map[string]any)
		if !ok || m["hello"] != "world" {
			t.Errorf("Go worker received data = %v, want {hello:world}", data)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Go worker did not process the Node-produced job in time")
	}

	q, _ := NewQueue("interop-b", WithClient(client))
	defer q.Close()
	eventually(t, 3*time.Second, func() bool {
		return client.ZScore(ctx, q.keys.Completed(), jobID).Err() == nil
	})
}
