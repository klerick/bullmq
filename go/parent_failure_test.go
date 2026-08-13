package bullmq

import (
	"errors"
	"testing"
)

// Enabling a parent-failure option must store its short flag inside the job's
// parent record — that record (hash field "parent") is the only place the Lua
// scripts read these flags from. Ported from src/classes/job.ts:214-234.
func TestNewJobParentFailureFlags(t *testing.T) {
	cases := []struct {
		name  string
		opts  JobOptions
		short string
	}{
		{"failParentOnFailure", JobOptions{FailParentOnFailure: true}, "fpof"},
		{"removeDependencyOnFailure", JobOptions{RemoveDependencyOnFailure: true}, "rdof"},
		{"ignoreDependencyOnFailure", JobOptions{IgnoreDependencyOnFailure: true}, "idof"},
		{"continueParentOnFailure", JobOptions{ContinueParentOnFailure: true}, "cpof"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := tc.opts
			o.Parent = &ParentOptions{ID: "p1", Queue: "bull:parents"}
			j := mustNewJob(t, nil, "child", nil, &o)
			if j.Parent[tc.short] != true {
				t.Errorf("Parent[%q] = %v, want true (parent = %v)", tc.short, j.Parent[tc.short], j.Parent)
			}
			// The option is also kept in opts, as upstream stores it (encodeOpts
			// shortens it at pack time), so it round-trips through Redis.
			if j.opts[tc.name] != true {
				t.Errorf("opts[%q] = %v, want true", tc.name, j.opts[tc.name])
			}
		})
	}
}

// Without any flag the parent record stays minimal: exactly {id, queueKey}. This
// is the upstream default and the reason an unflagged parent waits forever.
func TestNewJobParentNoFailureFlags(t *testing.T) {
	j := mustNewJob(t, nil, "child", nil, &JobOptions{
		Parent: &ParentOptions{ID: "p1", Queue: "bull:parents"},
	})
	if len(j.Parent) != 2 || j.Parent["id"] != "p1" || j.Parent["queueKey"] != "bull:parents" {
		t.Errorf("Parent = %v, want exactly {id:p1, queueKey:bull:parents}", j.Parent)
	}
}

// The flags reach Lua only through the parent record, so without a parent there
// is nothing to write — but that is not an error (upstream ignores them too).
func TestNewJobFailureFlagWithoutParent(t *testing.T) {
	j := mustNewJob(t, nil, "orphan", nil, &JobOptions{FailParentOnFailure: true})
	if j.Parent != nil {
		t.Errorf("Parent = %v, want nil", j.Parent)
	}
}

// The four options are mutually exclusive; combining them is a config error, not
// a silent last-one-wins. Ported from python/bullmq/job.py:66-77.
func TestNewJobExclusiveParentOptions(t *testing.T) {
	_, err := newJob(nil, "child", nil, &JobOptions{
		Parent:                  &ParentOptions{ID: "p1", Queue: "bull:parents"},
		FailParentOnFailure:     true,
		ContinueParentOnFailure: true,
	})
	if !errors.Is(err, ErrExclusiveParentOptions) {
		t.Fatalf("err = %v, want ErrExclusiveParentOptions", err)
	}
}

// Validation follows python: it runs even when no parent is set, so a bad combo
// is reported where it is written rather than silently ignored.
func TestNewJobExclusiveParentOptionsWithoutParent(t *testing.T) {
	_, err := newJob(nil, "orphan", nil, &JobOptions{
		IgnoreDependencyOnFailure: true,
		RemoveDependencyOnFailure: true,
	})
	if !errors.Is(err, ErrExclusiveParentOptions) {
		t.Fatalf("err = %v, want ErrExclusiveParentOptions", err)
	}
}

// Regression: the pre-existing escape hatch (Extra with the upstream long key, or
// the short stored key) used to reach opts and be read by nobody. It must now
// work exactly like the typed field.
func TestNewJobParentFailureFlagsFromExtra(t *testing.T) {
	for _, key := range []string{"failParentOnFailure", "fpof"} {
		t.Run(key, func(t *testing.T) {
			j := mustNewJob(t, nil, "child", nil, &JobOptions{
				Parent: &ParentOptions{ID: "p1", Queue: "bull:parents"},
				Extra:  map[string]any{key: true},
			})
			if j.Parent["fpof"] != true {
				t.Errorf("Parent = %v, want fpof:true", j.Parent)
			}
		})
	}
}

// Exclusivity is checked over the union of typed fields and Extra, so mixing the
// two sources cannot smuggle a forbidden combination through.
func TestNewJobExclusiveParentOptionsAcrossExtra(t *testing.T) {
	_, err := newJob(nil, "child", nil, &JobOptions{
		Parent:              &ParentOptions{ID: "p1", Queue: "bull:parents"},
		FailParentOnFailure: true,
		Extra:               map[string]any{"ignoreDependencyOnFailure": true},
	})
	if !errors.Is(err, ErrExclusiveParentOptions) {
		t.Fatalf("err = %v, want ErrExclusiveParentOptions", err)
	}
}

// keepLogs had the same shape of gap: Job.Log reads opts["keepLogs"], but
// JobOptions could not express it.
func TestNewJobKeepLogs(t *testing.T) {
	j := mustNewJob(t, nil, "j", nil, &JobOptions{KeepLogs: 10})
	if j.opts["keepLogs"] != 10 {
		t.Errorf("opts[keepLogs] = %v, want 10", j.opts["keepLogs"])
	}
	plain := mustNewJob(t, nil, "j", nil, nil)
	if _, ok := plain.opts["keepLogs"]; ok {
		t.Errorf("keepLogs must stay unset when zero, got %v", plain.opts["keepLogs"])
	}
}

// A parent moved back to wait by the fpof cascade carries the reason in "defa";
// the worker (not Lua) is what turns it into a failure, so the field has to
// survive the raw hash -> Job reconstruction. Mirrors src/classes/job.ts:378.
func TestJobFromRawDeferredFailure(t *testing.T) {
	j := jobFromRaw(nil, map[string]string{
		"name": "parent",
		"defa": "child bull:child:1 failed",
	}, "p1")
	if j.DeferredFailure != "child bull:child:1 failed" {
		t.Errorf("DeferredFailure = %q, want %q", j.DeferredFailure, "child bull:child:1 failed")
	}
}
