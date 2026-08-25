package bullmq

import (
	"context"
	"testing"
)

// The trace context a producer put on a job must be readable from the job itself.
// A queue-event listener has only a job id, and the event stream carries no trace
// context at all, so this accessor is the only way for it to continue the trace.
func TestJobTelemetryMetadata(t *testing.T) {
	h := telemetryHelper{t: &pathTelemetry{}, name: "q"}
	ctx, span := h.start(context.Background(), SpanKindProducer, "add")
	defer span.finish(nil)

	job := mustNewJob(t, nil, "task", nil, nil)
	h.injectTM(ctx, job.opts)
	if got := job.TelemetryMetadata(); got != "q.add" {
		t.Errorf("TelemetryMetadata = %q, want %q", got, "q.add")
	}

	plain := mustNewJob(t, nil, "task", nil, nil)
	if got := plain.TelemetryMetadata(); got != "" {
		t.Errorf("TelemetryMetadata = %q for a job added without telemetry, want empty", got)
	}
}

// The listener's path is a job re-read from Redis, so the accessor must work on a
// reconstructed job too, not only on one just built by a producer.
func TestJobTelemetryMetadataFromRaw(t *testing.T) {
	j := jobFromRaw(nil, map[string]string{
		"name": "task",
		"opts": `{"attempts":1,"tm":"00-4bf92f-00f067aa-01"}`,
	}, "j1")
	if got := j.TelemetryMetadata(); got != "00-4bf92f-00f067aa-01" {
		t.Errorf("TelemetryMetadata = %q, want the stored tm", got)
	}
}
