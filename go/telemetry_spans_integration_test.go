package bullmq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// spanRec is one started span: its name, kind, the ">"-joined path of the spans
// it nests in, and the error it ended with (nil when it ended clean).
type spanRec struct {
	name string
	kind SpanKind
	path string
	err  error
}

// recordingTelemetry captures the span tree so a test can assert shape, not just
// presence: which span nests in which, and which ones recorded an error.
type recordingTelemetry struct {
	mu   sync.Mutex
	recs []*spanRec
}

func (r *recordingTelemetry) StartSpan(ctx context.Context, name string, kind SpanKind) (context.Context, Span) {
	path := name
	if prev, _ := ctx.Value(spanPathKey{}).(string); prev != "" {
		path = prev + ">" + name
	}
	rec := &spanRec{name: name, kind: kind, path: path}
	r.mu.Lock()
	r.recs = append(r.recs, rec)
	r.mu.Unlock()
	return context.WithValue(ctx, spanPathKey{}, path), &recordingSpan{rec: rec, tel: r}
}

func (r *recordingTelemetry) Inject(ctx context.Context) string {
	s, _ := ctx.Value(spanPathKey{}).(string)
	return s
}

func (r *recordingTelemetry) Extract(ctx context.Context, metadata string) context.Context {
	return context.WithValue(ctx, spanPathKey{}, metadata)
}

// find returns the first recorded span with the given name.
func (r *recordingTelemetry) find(name string) *spanRec {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rec := range r.recs {
		if rec.name == name {
			return rec
		}
	}
	return nil
}

func (r *recordingTelemetry) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.recs))
	for _, rec := range r.recs {
		out = append(out, rec.path)
	}
	return out
}

type recordingSpan struct {
	rec *spanRec
	tel *recordingTelemetry
}

func (s *recordingSpan) SetAttributes(map[string]any) {}
func (s *recordingSpan) RecordError(err error) {
	s.tel.mu.Lock()
	s.rec.err = err
	s.tel.mu.Unlock()
}
func (s *recordingSpan) End() {}

// spanTestSetup wires a queue and a worker onto a fresh DB with recording telemetry.
func spanTestSetup(t *testing.T, ctx context.Context, name string, proc Processor, jobOpts *JobOptions) *recordingTelemetry {
	t.Helper()
	client := requireRedis(t)
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	tel := &recordingTelemetry{}
	q, err := NewQueue(name, WithClient(client), WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	if _, err := q.Add(ctx, "task", nil, jobOpts); err != nil {
		t.Fatalf("Add: %v", err)
	}
	w, err := NewWorker(name, proc, WithClient(client), WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	go func() { _ = w.Run(ctx) }()
	return tel
}

// Completing a job opens an INTERNAL span nested in the consumer's process span,
// as upstream does (job.ts:646 'complete' inside worker.ts:931 'process').
func TestCompleteSpanNestsUnderProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan struct{}, 1)
	tel := spanTestSetup(t, ctx, "span-complete", func(ctx context.Context, j *Job) (any, error) {
		done <- struct{}{}
		return "ok", nil
	}, nil)

	<-done
	eventually(t, 5*time.Second, func() bool { return tel.find("span-complete.complete") != nil })

	rec := tel.find("span-complete.complete")
	if rec.kind != SpanKindInternal {
		t.Errorf("complete span kind = %v, want internal", rec.kind)
	}
	if !strings.HasSuffix(rec.path, "span-complete.process>span-complete.complete") {
		t.Errorf("complete span path = %q, want it nested under the process span (all: %v)", rec.path, tel.names())
	}
}

// A job that has no attempts left ends in a 'fail' span; one that will be retried
// after a backoff ends in 'delay', and one retried immediately in 'retry'. Names
// come from upstream's getSpanOperation (job.ts:812-822).
func TestFailSpanNamedByOutcome(t *testing.T) {
	cases := []struct {
		name string
		opts *JobOptions
		want string
	}{
		{"fail", &JobOptions{Attempts: 1}, "span-fail.fail"},
		{"delay", &JobOptions{Attempts: 3, Backoff: &BackoffOptions{Type: "fixed", Delay: 30000}}, "span-fail.delay"},
		{"retry", &JobOptions{Attempts: 3}, "span-fail.retry"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			tel := spanTestSetup(t, ctx, "span-fail", failingProcessor, tc.opts)
			eventually(t, 5*time.Second, func() bool { return tel.find(tc.want) != nil })

			rec := tel.find(tc.want)
			if rec.kind != SpanKindInternal {
				t.Errorf("%s span kind = %v, want internal", tc.want, rec.kind)
			}
			if !strings.HasSuffix(rec.path, "span-fail.process>"+tc.want) {
				t.Errorf("%s span path = %q, want it nested under the process span (all: %v)", tc.want, rec.path, tel.names())
			}
		})
	}
}

// A processor parking its job in waiting-children is not a failure: upstream
// short-circuits before job.moveToFailed and records nothing on the span
// (worker.ts:1093-1107), so neither should the port.
func TestWaitingChildrenIsNotASpanError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan struct{}, 1)
	tel := spanTestSetup(t, ctx, "span-wc", func(ctx context.Context, j *Job) (any, error) {
		done <- struct{}{}
		return nil, ErrWaitingChildren
	}, nil)

	<-done
	eventually(t, 5*time.Second, func() bool { return tel.find("span-wc.process") != nil })

	rec := tel.find("span-wc.process")
	if rec.err != nil {
		t.Errorf("process span recorded %v, want no error for a parked job", rec.err)
	}
	if tel.find("span-wc.fail") != nil {
		t.Errorf("a parked job must not open a fail span (all: %v)", tel.names())
	}
	_ = errors.Is(ErrWaitingChildren, ErrWaitingChildren)
}
