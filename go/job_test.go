package bullmq

import "testing"

func TestNewJobBasics(t *testing.T) {
	j := newJob(nil, "createUser", map[string]any{"email": "a@b.c"}, &JobOptions{
		JobID:    "custom-1",
		Delay:    5000,
		Attempts: 3,
	})
	if j.ID != "custom-1" {
		t.Errorf("ID = %q, want %q", j.ID, "custom-1")
	}
	if j.Name != "createUser" {
		t.Errorf("Name = %q, want %q", j.Name, "createUser")
	}
	if j.Delay != 5000 {
		t.Errorf("Delay = %d, want 5000", j.Delay)
	}
	if j.Timestamp <= 0 {
		t.Errorf("Timestamp should default to now, got %d", j.Timestamp)
	}
}

// A nil opts pointer must be handled (defaults applied).
func TestNewJobNilOpts(t *testing.T) {
	j := newJob(nil, "noop", nil, nil)
	if j.Timestamp <= 0 {
		t.Error("Timestamp should default to now for nil opts")
	}
	if j.ID != "" {
		t.Errorf("ID should be empty for nil opts, got %q", j.ID)
	}
}

// Parent options resolve to a parentKey ("{queueKey}:{id}") and a parent record
// {id, queueKey}, mirroring python get_parent_key + Job.parent.
func TestNewJobParent(t *testing.T) {
	j := newJob(nil, "child", nil, &JobOptions{
		Parent: &ParentOptions{ID: "p1", Queue: "bull:parents"},
	})
	if j.ParentKey != "bull:parents:p1" {
		t.Errorf("ParentKey = %q, want %q", j.ParentKey, "bull:parents:p1")
	}
	if j.Parent["id"] != "p1" || j.Parent["queueKey"] != "bull:parents" {
		t.Errorf("Parent = %v, want {id:p1, queueKey:bull:parents}", j.Parent)
	}
}

// optsMap carries the fields storeJob reads (attempts, delay); deduplication is
// stored under its long key so encodeOpts can shorten it to "de" at pack time.
func TestJobOptsMap(t *testing.T) {
	j := newJob(nil, "j", nil, &JobOptions{Attempts: 2, Delay: 100})
	m := j.optsMap()
	if m["attempts"] != 2 {
		t.Errorf("optsMap attempts = %v, want 2", m["attempts"])
	}
	if m["delay"] != int64(100) {
		t.Errorf("optsMap delay = %v (%T), want int64(100)", m["delay"], m["delay"])
	}
}
