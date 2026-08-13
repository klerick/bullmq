package bullmq

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// The packed args array must follow the 9-slot cross-port contract. addJobArgs
// touches only s.keys, so it needs no live connection.
func TestAddJobArgsStructure(t *testing.T) {
	s := &scripts{keys: NewQueueKeys("q", "")}
	job := mustNewJob(t, nil, "createUser", map[string]any{"e": "x"}, &JobOptions{JobID: "j1"})

	argv, err := s.addJobArgs(job)
	if err != nil {
		t.Fatalf("addJobArgs: %v", err)
	}
	if len(argv) != 3 {
		t.Fatalf("addJobArgs returned %d ARGV, want 3", len(argv))
	}

	// ARGV[2] is the raw JSON data string.
	if data, ok := argv[1].(string); !ok || data != `{"e":"x"}` {
		t.Errorf("ARGV[2] data = %v, want %q", argv[1], `{"e":"x"}`)
	}

	// ARGV[1] is the packed 9-element args array.
	packed, ok := argv[0].([]byte)
	if !ok {
		t.Fatalf("ARGV[1] is %T, want []byte", argv[0])
	}
	var arr []any
	if err := msgpack.Unmarshal(packed, &arr); err != nil {
		t.Fatalf("unmarshal packed args: %v", err)
	}
	if len(arr) != 9 {
		t.Fatalf("packed args len = %d, want 9", len(arr))
	}
	if arr[0] != "bull:q:" {
		t.Errorf("args[1] keyPrefix = %v, want %q", arr[0], "bull:q:")
	}
	if arr[1] != "j1" {
		t.Errorf("args[2] id = %v, want %q", arr[1], "j1")
	}
	if arr[2] != "createUser" {
		t.Errorf("args[3] name = %v, want %q", arr[2], "createUser")
	}
	if arr[3] != job.Timestamp {
		t.Errorf("args[4] timestamp = %v (%T), want %d", arr[3], arr[3], job.Timestamp)
	}
	// A leaf job has no parent/dedup/repeat — positions 5..9 must be nil.
	for i := 4; i < 9; i++ {
		if arr[i] != nil {
			t.Errorf("args[%d] = %v, want nil", i+1, arr[i])
		}
	}
}

// Without a custom id, args[2] must be the empty string so the Lua script INCRs one
// (a nil there would be treated as a real, malformed id).
func TestAddJobArgsEmptyID(t *testing.T) {
	s := &scripts{keys: NewQueueKeys("q", "")}
	job := mustNewJob(t, nil, "n", nil, nil)
	argv, _ := s.addJobArgs(job)
	var arr []any
	_ = msgpack.Unmarshal(argv[0].([]byte), &arr)
	if arr[1] != "" {
		t.Errorf("args[2] = %v (%T), want empty string", arr[1], arr[1])
	}
}
