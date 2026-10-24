package bullmq

import (
	"errors"
	"reflect"
	"testing"
)

// Upstream merges queue defaults with call options by spreading two objects
// (`{...this.jobsOpts, ...opts, jobId}`, queue.ts:381-385), where a missing key and
// a key set to its zero value are different things. A Go struct cannot tell them
// apart, so MergeJobOptions reads a zero scalar as "not set" and inherits the
// default. These tests pin every rule of that translation, because the consumer
// relies on the same function for its own layer (app defaults <- queue defaults).

// Rule 1: a scalar left at its zero value inherits the default.
func TestMergeJobOptionsInheritsUnsetScalars(t *testing.T) {
	base := &JobOptions{Delay: 500, Priority: 3, Attempts: 5, Timestamp: 1700000000000,
		Lifo: true, SizeLimit: 1024, KeepLogs: 20}
	got := MergeJobOptions(base, &JobOptions{})

	want := *base
	want.JobID = "" // rule 6, checked on its own below
	if !reflect.DeepEqual(*got, want) {
		t.Errorf("merged = %+v, want the defaults verbatim %+v", *got, want)
	}
}

// Rule 1, other half: a scalar the caller set wins over the default.
func TestMergeJobOptionsCallWinsOverDefaults(t *testing.T) {
	base := &JobOptions{Delay: 500, Priority: 3, Attempts: 5, SizeLimit: 1024, KeepLogs: 20}
	over := &JobOptions{Delay: 10, Priority: 9, Attempts: 1, SizeLimit: 2048, KeepLogs: 7}
	got := MergeJobOptions(base, over)

	if got.Delay != 10 || got.Priority != 9 || got.Attempts != 1 || got.SizeLimit != 2048 || got.KeepLogs != 7 {
		t.Errorf("merged = %+v, want the call's scalars %+v", *got, *over)
	}
}

// The accepted cost of rule 1: an explicit zero cannot override a non-zero
// default, because the two are the same value in a Go struct. Extra is the escape
// hatch — it is applied last in buildOptsMap, so it reaches the stored opts even
// when the typed field has already been merged.
func TestMergeJobOptionsZeroDoesNotOverrideButExtraDoes(t *testing.T) {
	base := &JobOptions{Attempts: 3}

	if got := MergeJobOptions(base, &JobOptions{Attempts: 0}); got.Attempts != 3 {
		t.Errorf("Attempts = %d, want the default 3 kept (zero means unset)", got.Attempts)
	}

	merged := MergeJobOptions(base, &JobOptions{Extra: map[string]any{"attempts": 0}})
	opts := buildOptsMap(merged, nil)
	if opts["attempts"] != 0 {
		t.Errorf("stored attempts = %v, want 0 — Extra must be able to express an explicit zero", opts["attempts"])
	}
}

// Rule 2: a pointer field is nil-or-whole, like spreading a nested object. The
// caller's Backoff replaces the default's entirely rather than field by field.
func TestMergeJobOptionsPointersAreWholeValues(t *testing.T) {
	base := &JobOptions{Backoff: &BackoffOptions{Type: "exponential", Delay: 1000}}

	if got := MergeJobOptions(base, &JobOptions{}); got.Backoff != base.Backoff {
		t.Errorf("Backoff = %+v, want the default inherited", got.Backoff)
	}

	over := &JobOptions{Backoff: &BackoffOptions{Type: "fixed"}}
	got := MergeJobOptions(base, over)
	if got.Backoff.Type != "fixed" || got.Backoff.Delay != 0 {
		t.Errorf("Backoff = %+v, want the call's value whole (no Delay carried over)", got.Backoff)
	}
}

// Rule 3: removeOnComplete/removeOnFail are `any`, so they CAN tell an explicit
// false or 0 from an absent value — and must.
func TestMergeJobOptionsRemoveOnDistinguishesExplicitZero(t *testing.T) {
	base := &JobOptions{RemoveOnComplete: true, RemoveOnFail: 5}

	if got := MergeJobOptions(base, &JobOptions{}); got.RemoveOnComplete != true || got.RemoveOnFail != 5 {
		t.Errorf("merged = %+v %+v, want the defaults inherited", got.RemoveOnComplete, got.RemoveOnFail)
	}

	got := MergeJobOptions(base, &JobOptions{RemoveOnComplete: false, RemoveOnFail: 0})
	if got.RemoveOnComplete != false || got.RemoveOnFail != 0 {
		t.Errorf("merged = %v %v, want the call's explicit false/0", got.RemoveOnComplete, got.RemoveOnFail)
	}
}

// Rule 4: the four parent-failure flags are one decision, so the caller's choice
// replaces the default's as a group. Merging them field by field would leave both
// set and make a perfectly sensible override fail validation.
//
// This is a deliberate departure from a literal spread: TS and python merge the
// keys and then throw (job.ts:1333-1355, job.py:66-77). They can afford to,
// because there the default can be switched off with `failParentOnFailure: false`;
// in Go that value is indistinguishable from "not set", so the group rule is what
// takes its place.
func TestMergeJobOptionsReplacesParentFailureAsAGroup(t *testing.T) {
	base := &JobOptions{FailParentOnFailure: true}
	got := MergeJobOptions(base, &JobOptions{ContinueParentOnFailure: true})

	if got.FailParentOnFailure {
		t.Error("FailParentOnFailure is still set, want the default's policy dropped")
	}
	if !got.ContinueParentOnFailure {
		t.Error("ContinueParentOnFailure is not set, want the call's policy kept")
	}
	if _, err := resolveParentFailureOptions(got); err != nil {
		t.Errorf("merged options do not validate: %v", err)
	}
}

// The group rule has to work through Extra too, in both directions: Extra is how
// these flags were set before the typed fields existed, and enabled() still reads
// it (job.go:104-116).
func TestMergeJobOptionsParentFailureGroupThroughExtra(t *testing.T) {
	// Default set through Extra, call through its typed field.
	got := MergeJobOptions(
		&JobOptions{Extra: map[string]any{"failParentOnFailure": true, "keepLogs": 3}},
		&JobOptions{ContinueParentOnFailure: true})
	if _, ok := got.Extra["failParentOnFailure"]; ok {
		t.Error("Extra still carries failParentOnFailure, want the default's policy dropped")
	}
	if got.Extra["keepLogs"] != 3 {
		t.Error("unrelated Extra key was dropped with the group")
	}
	if err := errFor(got); err != nil {
		t.Errorf("merged options do not validate: %v", err)
	}

	// Default set through its typed field, call through the short Extra key.
	got = MergeJobOptions(
		&JobOptions{FailParentOnFailure: true},
		&JobOptions{Extra: map[string]any{"cpof": true}})
	if got.FailParentOnFailure {
		t.Error("FailParentOnFailure is still set, want the default's policy dropped")
	}
	if err := errFor(got); err != nil {
		t.Errorf("merged options do not validate: %v", err)
	}

	// Default set through the short key, call through the long one.
	got = MergeJobOptions(
		&JobOptions{Extra: map[string]any{"fpof": true}},
		&JobOptions{Extra: map[string]any{"continueParentOnFailure": true}})
	if _, ok := got.Extra["fpof"]; ok {
		t.Error("Extra still carries fpof, want the default's policy dropped")
	}
	if err := errFor(got); err != nil {
		t.Errorf("merged options do not validate: %v", err)
	}
}

// errFor reports whether the merged options would be rejected as a combination of
// exclusive parent-failure policies.
func errFor(o *JobOptions) error {
	_, err := resolveParentFailureOptions(o)
	if errors.Is(err, ErrExclusiveParentOptions) {
		return err
	}
	return nil
}

// A call that chooses no parent-failure policy inherits the default's, as any
// other field.
func TestMergeJobOptionsKeepsParentFailureWhenCallChoosesNone(t *testing.T) {
	got := MergeJobOptions(&JobOptions{FailParentOnFailure: true}, &JobOptions{Priority: 2})
	if !got.FailParentOnFailure {
		t.Error("FailParentOnFailure was dropped, want the default inherited")
	}

	got = MergeJobOptions(&JobOptions{Extra: map[string]any{"idof": true}}, &JobOptions{Priority: 2})
	if got.Extra["idof"] != true {
		t.Error("Extra idof was dropped, want the default inherited")
	}
}

// Rule 5: Extra holds top-level upstream option keys, so it merges per key with
// the call winning — the same shape as the spread it stands for.
func TestMergeJobOptionsMergesExtraPerKey(t *testing.T) {
	base := &JobOptions{Extra: map[string]any{"a": 1, "b": 2}}
	over := &JobOptions{Extra: map[string]any{"b": 3, "c": 4}}
	got := MergeJobOptions(base, over)

	want := map[string]any{"a": 1, "b": 3, "c": 4}
	if !reflect.DeepEqual(got.Extra, want) {
		t.Errorf("Extra = %v, want %v", got.Extra, want)
	}
}

// Rule 6: jobId comes from the call only. Upstream spells it out by re-assigning
// the key after the spread (`{...this.jobsOpts, ...opts, jobId}`), so a default
// job id cannot silently collide every job of the queue onto one.
func TestMergeJobOptionsTakesJobIDFromCallOnly(t *testing.T) {
	got := MergeJobOptions(&JobOptions{JobID: "default-id"}, &JobOptions{})
	if got.JobID != "" {
		t.Errorf("JobID = %q, want empty — a default job id must not apply", got.JobID)
	}
	got = MergeJobOptions(&JobOptions{JobID: "default-id"}, &JobOptions{JobID: "mine"})
	if got.JobID != "mine" {
		t.Errorf("JobID = %q, want %q", got.JobID, "mine")
	}
}

// nil is the normal case on both sides: a queue with no defaults, or a job added
// with no options. Two nils stay nil so the "no options at all" path downstream is
// unchanged.
func TestMergeJobOptionsNilArguments(t *testing.T) {
	if got := MergeJobOptions(nil, nil); got != nil {
		t.Errorf("MergeJobOptions(nil, nil) = %+v, want nil", got)
	}

	over := &JobOptions{Attempts: 2}
	got := MergeJobOptions(nil, over)
	if got == over {
		t.Error("MergeJobOptions(nil, over) returned the caller's pointer, want a copy")
	}
	if got.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2", got.Attempts)
	}

	base := &JobOptions{Attempts: 3, JobID: "x"}
	got = MergeJobOptions(base, nil)
	if got == base {
		t.Error("MergeJobOptions(base, nil) returned the caller's pointer, want a copy")
	}
	if got.Attempts != 3 || got.JobID != "" {
		t.Errorf("merged = %+v, want the defaults with no job id", *got)
	}
}

// Neither argument may be touched: the defaults belong to a Queue that keeps
// adding jobs, and the call options belong to the caller. python guards the same
// invariant explicitly (flow_producer.py:103-107).
func TestMergeJobOptionsDoesNotMutateItsArguments(t *testing.T) {
	base := &JobOptions{Attempts: 3, FailParentOnFailure: true,
		Extra: map[string]any{"a": 1, "fpof": true}}
	over := &JobOptions{Priority: 2, ContinueParentOnFailure: true,
		Extra: map[string]any{"b": 2}}
	baseCopy, overCopy := *base, *over
	baseExtra := map[string]any{"a": 1, "fpof": true}
	overExtra := map[string]any{"b": 2}

	merged := MergeJobOptions(base, over)
	merged.Attempts = 99
	merged.Extra["c"] = 3

	if base.Attempts != baseCopy.Attempts || !base.FailParentOnFailure {
		t.Errorf("base was mutated: %+v", *base)
	}
	if over.Priority != overCopy.Priority {
		t.Errorf("over was mutated: %+v", *over)
	}
	if !reflect.DeepEqual(base.Extra, baseExtra) {
		t.Errorf("base.Extra was mutated: %v, want %v", base.Extra, baseExtra)
	}
	if !reflect.DeepEqual(over.Extra, overExtra) {
		t.Errorf("over.Extra was mutated: %v, want %v", over.Extra, overExtra)
	}
}

// The getter hands out a copy, as upstream's does (queue.ts:241-243), so a caller
// inspecting the defaults cannot change what the queue will apply to its next job.
// Extra is copied too: upstream's spread is shallow, but there a nested object is
// the caller's own literal, while here it is the only route by which a caller could
// reach into the queue's stored options.
func TestQueueDefaultJobOptionsReturnsACopy(t *testing.T) {
	defaults := &JobOptions{Attempts: 3, Extra: map[string]any{"a": 1}}
	q := &Queue{defaultJobOptions: defaults}

	got := q.DefaultJobOptions()
	if got == defaults {
		t.Fatal("DefaultJobOptions returned the queue's own pointer, want a copy")
	}
	got.Attempts = 99
	got.Extra["a"] = 2

	if defaults.Attempts != 3 {
		t.Errorf("queue defaults were mutated: Attempts = %d, want 3", defaults.Attempts)
	}
	if defaults.Extra["a"] != 1 {
		t.Errorf("queue defaults were mutated through Extra: a = %v, want 1", defaults.Extra["a"])
	}

	if (&Queue{}).DefaultJobOptions() != nil {
		t.Error("a queue with no defaults must report nil, not an empty value")
	}
}

// WithDefaultJobOptions reaches config like any other option, so NewWorker and
// NewFlowProducer — which take the same option slice — see it too. Neither has
// defaults of its own in any port, so both must ignore it; this pins the wiring
// and FlowProducer's side is checked end to end in the integration test.
func TestWithDefaultJobOptionsSetsConfig(t *testing.T) {
	defaults := &JobOptions{Attempts: 4}
	cfg := newConfig(WithDefaultJobOptions(defaults))
	if cfg.defaultJobOptions != defaults {
		t.Errorf("cfg.defaultJobOptions = %+v, want the given defaults", cfg.defaultJobOptions)
	}
}
