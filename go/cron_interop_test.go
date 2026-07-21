package bullmq

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"
)

// cronNextMillis must agree with Node's cron-parser (the library BullMQ uses),
// so cron schedules created by one runtime advance identically in the other.
func TestCronNextMatchesNode(t *testing.T) {
	client := requireRedis(t) // only used as an availability gate
	t.Cleanup(func() { _ = client.Close() })
	requireNodeInterop(t)

	// A fixed reference time so the comparison is deterministic (2023-11-14T22:13:20Z).
	const after int64 = 1700000000000

	patterns := []struct {
		pattern string
		tz      string
	}{
		{"*/5 * * * *", ""},             // every 5 minutes
		{"0 9 * * *", ""},               // 09:00 daily
		{"0 0 1 * *", ""},               // midnight on the 1st
		{"30 8 * * 1", ""},              // Mondays 08:30
		{"15 14 1 * *", ""},             // 14:15 on the 1st
		{"0 22 * * 1-5", ""},            // 22:00 on weekdays
		{"*/30 * * * * *", ""},          // every 30 seconds (6-field)
		{"0 12 * * *", "Europe/Berlin"}, // noon Berlin time
	}

	for _, p := range patterns {
		got, err := cronNextMillis(p.pattern, p.tz, after)
		if err != nil {
			t.Errorf("cronNextMillis(%q, %q): %v", p.pattern, p.tz, err)
			continue
		}
		out := strings.TrimSpace(runNode(t, "cronnext.mjs", p.pattern, p.tz, strconv.FormatInt(after, 10)))
		want, err := strconv.ParseInt(out, 10, 64)
		if err != nil {
			t.Errorf("node cronnext for %q returned %q: %v", p.pattern, out, err)
			continue
		}
		if got != want {
			t.Errorf("cron %q tz=%q: Go=%d Node=%d (diff %dms)", p.pattern, p.tz, got, want, got-want)
		}
	}
}

// A second-granularity cron scheduler keeps producing jobs.
func TestCronSchedulerRecurring(t *testing.T) {
	client := requireRedis(t)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	q, _ := NewQueue("go-cron", WithClient(client))
	defer q.Close()

	job, err := q.UpsertJobScheduler(ctx, "everysec", RepeatOptions{Pattern: "*/1 * * * * *"}, "tick", nil, nil)
	if err != nil {
		t.Fatalf("UpsertJobScheduler(pattern): %v", err)
	}
	if !strings.HasPrefix(job.ID, "repeat:everysec:") {
		t.Errorf("cron job id = %q, want repeat:everysec:<millis>", job.ID)
	}
	if sched, _ := q.GetJobScheduler(ctx, "everysec"); sched == nil || sched.Pattern != "*/1 * * * * *" {
		t.Errorf("stored cron scheduler = %+v", sched)
	}

	var count int
	enough := make(chan struct{}, 1)
	var mu = make(chan struct{}, 1)
	mu <- struct{}{}
	w, _ := NewWorker("go-cron", func(ctx context.Context, j *Job) (any, error) {
		<-mu
		count++
		n := count
		mu <- struct{}{}
		if n == 3 {
			select {
			case enough <- struct{}{}:
			default:
			}
		}
		return "ok", nil
	}, WithClient(client))
	defer w.Close()
	go func() { _ = w.Run(ctx) }()

	select {
	case <-enough:
	case <-time.After(10 * time.Second):
		<-mu
		n := count
		t.Fatalf("cron scheduler produced only %d jobs, want >= 3", n)
	}
}
