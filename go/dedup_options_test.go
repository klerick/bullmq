package bullmq

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// Every deduplication option the Lua scripts read must reach them, and the ones
// the caller did not set must stay absent: an explicit zero (ttl: 0) is a
// different branch in the script than "no ttl at all".
func TestDeduplicationOptionsPacking(t *testing.T) {
	full := mustNewJob(t, nil, "task", nil, &JobOptions{
		Deduplication: &DeduplicationOptions{
			ID: "k", TTL: 5000, Extend: true, Replace: true, KeepLastIfActive: true,
		},
	})
	want := map[string]any{
		"id": "k", "ttl": int64(5000), "extend": true, "replace": true, "keepLastIfActive": true,
	}
	if got := full.opts["deduplication"]; !reflect.DeepEqual(got, want) {
		t.Errorf("deduplication = %v, want %v", got, want)
	}

	bare := mustNewJob(t, nil, "task", nil, &JobOptions{
		Deduplication: &DeduplicationOptions{ID: "k"},
	})
	if got := bare.opts["deduplication"]; !reflect.DeepEqual(got, map[string]any{"id": "k"}) {
		t.Errorf("deduplication = %v, want only the id", got)
	}
}

// Extra stays the escape hatch it has always been: it is merged last and replaces
// the whole deduplication map.
func TestDeduplicationExtraStillWins(t *testing.T) {
	j := mustNewJob(t, nil, "task", nil, &JobOptions{
		Deduplication: &DeduplicationOptions{ID: "typed"},
		Extra:         map[string]any{"deduplication": map[string]any{"id": "from-extra"}},
	})
	got, _ := j.opts["deduplication"].(map[string]any)
	if got["id"] != "from-extra" {
		t.Errorf("deduplication = %v, want the Extra value to win", j.opts["deduplication"])
	}
}

// replace means "replace the delayed record of the job holding the key", which a
// flow node cannot express: upstream types it out (FlowDeduplicationOptions =
// Omit<DeduplicationOptions, 'replace'>). Refused before any server call.
func TestFlowAddRejectsDeduplicationReplace(t *testing.T) {
	fp, err := NewFlowProducer(WithClient(offlineClient(t)))
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}

	cases := map[string]*JobOptions{
		"typed field": {Deduplication: &DeduplicationOptions{ID: "k", Replace: true}},
		"via Extra":   {Extra: map[string]any{"deduplication": map[string]any{"id": "k", "replace": true}}},
		"short key":   {Extra: map[string]any{"de": map[string]any{"id": "k", "replace": true}}},
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := fp.Add(context.Background(), &FlowJob{
				QueueName: "root-q", Name: "root", Opts: opts,
				Children: []*FlowJob{{QueueName: "child-q", Name: "child"}},
			})
			if !errors.Is(err, ErrDeduplicationReplaceInFlow) {
				t.Fatalf("err = %v, want ErrDeduplicationReplaceInFlow", err)
			}
		})
	}
}

// The other deduplication options are fine on the root of a flow.
func TestFlowAddAllowsOtherDeduplicationOptions(t *testing.T) {
	fp, err := NewFlowProducer(WithClient(offlineClient(t)))
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}
	_, err = fp.Add(context.Background(), &FlowJob{
		QueueName: "root-q", Name: "root",
		Opts: &JobOptions{Deduplication: &DeduplicationOptions{ID: "k", KeepLastIfActive: true, TTL: 100}},
	})
	if errors.Is(err, ErrDeduplicationReplaceInFlow) || errors.Is(err, ErrDeduplicationNotOnRoot) {
		t.Fatalf("root deduplication was rejected: %v", err)
	}
}

// A job read back from Redis knows its deduplication id: the hash carries it as
// "deid" and upstream exposes it (redis-queue-backend raw2jobData). A worker that
// wants to act on the key — release it early, log it — needs it.
func TestJobFromRawDeduplicationID(t *testing.T) {
	j := jobFromRaw(nil, map[string]string{"name": "task", "deid": "k-1"}, "j1")
	if j.DeduplicationID != "k-1" {
		t.Errorf("DeduplicationID = %q, want %q", j.DeduplicationID, "k-1")
	}
}
