package bullmq

import (
	"context"
	"errors"
	"testing"
)

// The rejection is a validation, not a Redis round trip: it fires before the
// producer talks to the server at all (this client would fail to dial).
func TestFlowAddRejectsNestedDeduplicationBeforeAnyCall(t *testing.T) {
	fp, err := NewFlowProducer(WithClient(offlineClient(t)))
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}

	cases := map[string]*JobOptions{
		"typed field": {Deduplication: &DeduplicationOptions{ID: "k"}},
		"via Extra":   {Extra: map[string]any{"deduplication": map[string]any{"id": "k"}}},
		"short key":   {Extra: map[string]any{"de": map[string]any{"id": "k"}}},
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := fp.Add(context.Background(), &FlowJob{
				QueueName: "root-q", Name: "root",
				Children: []*FlowJob{{QueueName: "child-q", Name: "child", Opts: opts}},
			})
			if !errors.Is(err, ErrDeduplicationNotOnRoot) {
				t.Fatalf("err = %v, want ErrDeduplicationNotOnRoot", err)
			}
		})
	}
}

// Deduplication on the root is the supported case and must not be rejected: with
// no server reachable it has to fail on the connection, not on validation.
func TestFlowAddAllowsDeduplicationOnRoot(t *testing.T) {
	fp, err := NewFlowProducer(WithClient(offlineClient(t)))
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}
	_, err = fp.Add(context.Background(), &FlowJob{
		QueueName: "root-q", Name: "root",
		Opts:     &JobOptions{Deduplication: &DeduplicationOptions{ID: "k"}},
		Children: []*FlowJob{{QueueName: "child-q", Name: "child"}},
	})
	if errors.Is(err, ErrDeduplicationNotOnRoot) {
		t.Fatalf("root deduplication was rejected: %v", err)
	}
}
