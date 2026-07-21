package bullmq

import "testing"

// Reference values ported from rust/src/keys.rs tests — this is the interop
// contract (keys must match Node/Rust/Python byte-for-byte).

func TestQueueKeysDefaultPrefix(t *testing.T) {
	k := NewQueueKeys("test-queue", "")
	cases := map[string]string{
		"base":      k.Base(),
		"keyPrefix": k.KeyPrefix(),
		"wait":      k.Wait(),
		"active":    k.Active(),
		"meta":      k.Meta(),
	}
	want := map[string]string{
		"base":      "bull:test-queue",
		"keyPrefix": "bull:test-queue:",
		"wait":      "bull:test-queue:wait",
		"active":    "bull:test-queue:active",
		"meta":      "bull:test-queue:meta",
	}
	for name, got := range cases {
		if got != want[name] {
			t.Errorf("%s: got %q, want %q", name, got, want[name])
		}
	}
}

func TestQueueKeysCustomPrefix(t *testing.T) {
	k := NewQueueKeys("my-queue", "myapp")
	if got := k.Base(); got != "myapp:my-queue" {
		t.Errorf("Base: got %q, want %q", got, "myapp:my-queue")
	}
	if got := k.Delayed(); got != "myapp:my-queue:delayed" {
		t.Errorf("Delayed: got %q, want %q", got, "myapp:my-queue:delayed")
	}
}

func TestQueueKeysJobKey(t *testing.T) {
	k := NewQueueKeys("q", "")
	if got := k.JobKey("123"); got != "bull:q:123" {
		t.Errorf("JobKey: got %q, want %q", got, "bull:q:123")
	}
}

// base64 of the queue name for CLIENT SETNAME — Node discovers workers via getWorkers
// (Buffer.from(name).toString('base64')). Values from rust test_base64_standard_matches_node.
func TestQueueNameBase64(t *testing.T) {
	cases := map[string]string{
		"test":     "dGVzdA==",
		"my-queue": "bXktcXVldWU=",
		"f":        "Zg==",
		"fo":       "Zm8=",
		"foo":      "Zm9v",
		"":         "",
	}
	for in, want := range cases {
		if got := nameToBase64(in); got != want {
			t.Errorf("nameToBase64(%q): got %q, want %q", in, got, want)
		}
	}
}

func TestQueueKeysClientName(t *testing.T) {
	k := NewQueueKeys("test", "bull")
	if got := k.ClientName(""); got != "bull:dGVzdA==" {
		t.Errorf("ClientName(\"\"): got %q, want %q", got, "bull:dGVzdA==")
	}
	if got := k.ClientName(":w:worker-1"); got != "bull:dGVzdA==:w:worker-1" {
		t.Errorf("ClientName suffix: got %q, want %q", got, "bull:dGVzdA==:w:worker-1")
	}
}

func TestValidateQueueName(t *testing.T) {
	if err := ValidateQueueName("ok"); err != nil {
		t.Errorf("valid name rejected: %v", err)
	}
	if err := ValidateQueueName(""); err == nil {
		t.Error("empty name should be rejected")
	}
	if err := ValidateQueueName("a:b"); err == nil {
		t.Error("name with colon should be rejected")
	}
}

// resolveParentQueueKey accepts both an unqualified name and an already-qualified
// key for the current prefix; extra colons are rejected. From rust keys.rs.
func TestResolveParentQueueKey(t *testing.T) {
	ok := []struct{ prefix, queue, want string }{
		{"bull", "parent-queue", "bull:parent-queue"},
		{"bull", "bull:parent-queue", "bull:parent-queue"},
	}
	for _, c := range ok {
		got, err := ResolveParentQueueKey(c.prefix, c.queue)
		if err != nil {
			t.Errorf("ResolveParentQueueKey(%q,%q) unexpected err: %v", c.prefix, c.queue, err)
		}
		if got != c.want {
			t.Errorf("ResolveParentQueueKey(%q,%q): got %q, want %q", c.prefix, c.queue, got, c.want)
		}
	}
	bad := []struct{ prefix, queue string }{
		{"bull", "parent:queue"},
		{"bull", "bull:parent:queue"},
	}
	for _, c := range bad {
		if _, err := ResolveParentQueueKey(c.prefix, c.queue); err == nil {
			t.Errorf("ResolveParentQueueKey(%q,%q) should error", c.prefix, c.queue)
		}
	}
}
