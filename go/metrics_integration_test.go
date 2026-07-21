package bullmq

import (
	"context"
	"testing"
	"time"
)

// With the metrics option, a worker records a completed-job time series that
// GetMetrics reads back as raw data.
func TestQueueMetrics(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-metrics", WithClient(client))
	defer q.Close()

	const n = 3
	for i := 0; i < n; i++ {
		if _, err := q.Add(ctx, "task", nil, nil); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	processed := make(chan struct{}, n)
	w, _ := NewWorker("go-metrics", func(ctx context.Context, j *Job) (any, error) {
		processed <- struct{}{}
		return nil, nil
	}, WithClient(client), WithMetrics(100))
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	for i := 0; i < n; i++ {
		select {
		case <-processed:
		case <-time.After(5 * time.Second):
			t.Fatal("jobs not processed in time")
		}
	}

	var m *Metrics
	eventually(t, 3*time.Second, func() bool {
		m, _ = q.GetMetrics(ctx, "completed", 0, -1)
		return m != nil && m.Count == n
	})
	if m == nil || m.Count != n {
		t.Fatalf("completed metrics count = %v, want %d", m, n)
	}
	// Note: Data is bucketed per minute, so a fast test that completes all jobs
	// within one minute records the count but not yet a flushed data point.

	// Invalid metric type is rejected.
	if _, err := q.GetMetrics(ctx, "bogus", 0, -1); err == nil {
		t.Error("GetMetrics with an invalid type should error")
	}
}
