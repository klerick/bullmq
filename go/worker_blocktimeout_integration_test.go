package bullmq

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// capturingLogger records what go-redis logs, so a test can assert on the absence of
// a specific line.
type capturingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *capturingLogger) Printf(_ context.Context, format string, v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, v...))
}

func (l *capturingLogger) Lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

// stderrLogger reproduces go-redis's own default logger, which lives in an internal
// package and cannot be read back to be restored verbatim.
type stderrLogger struct{ log *log.Logger }

func (l *stderrLogger) Printf(_ context.Context, format string, v ...interface{}) {
	_ = l.log.Output(2, fmt.Sprintf(format, v...))
}

// captureRedisLogger swaps go-redis's process-wide logger for a recording one, and
// puts an equivalent of the default back when the test ends.
func captureRedisLogger(t *testing.T) *capturingLogger {
	t.Helper()
	c := &capturingLogger{}
	redis.SetLogger(c)
	t.Cleanup(func() {
		redis.SetLogger(&stderrLogger{log: log.New(os.Stderr, "redis: ", log.LstdFlags|log.Lshortfile)})
	})
	return c
}

// A worker waiting for a delayed job less than a second away must block for exactly
// that long, against a real Redis, and must not make go-redis log the
// "minimal supported value is 1s - truncating to 1s" warning on every such wait.
func TestWorkerSubSecondBlockIsExactAndQuietIntegration(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()

	logs := captureRedisLogger(t)

	w, err := NewWorker("go-blocktimeout-test", nil, WithClient(client))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	// Nothing must be waiting on the marker, so the block runs its full timeout.
	if err := client.Del(ctx, w.queue.keys.Marker()).Err(); err != nil {
		t.Fatalf("clearing the marker: %v", err)
	}

	// Ask for a wake-up 350ms out, exactly as moveToActive does for a delayed job.
	w.blockUntil = nowMillis() + 350

	start := time.Now()
	w.waitForJob(ctx)
	elapsed := time.Since(start)

	if elapsed >= time.Second {
		t.Errorf("blocked for %s on a 350ms wake-up: the timeout was rounded up to a whole second", elapsed)
	}
	if elapsed < 250*time.Millisecond {
		t.Errorf("returned after %s, want ~350ms: the command did not block", elapsed)
	}
	for _, line := range logs.Lines() {
		if strings.Contains(line, "minimal supported value") {
			t.Errorf("go-redis logged a truncation warning: %s", line)
		}
	}
}
