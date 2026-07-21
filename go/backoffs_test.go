package bullmq

import "testing"

func TestNormalizeBackoff(t *testing.T) {
	m := normalizeBackoff(&BackoffOptions{Type: "exponential", Delay: 1000})
	if m["type"] != "exponential" || m["delay"] != int64(1000) {
		t.Errorf("normalizeBackoff = %v, want {exponential, 1000}", m)
	}
	if normalizeBackoff(nil) != nil {
		t.Error("normalizeBackoff(nil) should be nil")
	}
}

// Ported from python backoffs.py builtin_strategies.
func TestCalculateBackoff(t *testing.T) {
	fixed := map[string]any{"type": "fixed", "delay": int64(500)}
	if got := calculateBackoff(fixed, 3); got != 500 {
		t.Errorf("fixed backoff = %d, want 500", got)
	}

	exp := map[string]any{"type": "exponential", "delay": int64(1000)}
	cases := map[int]int64{1: 1000, 2: 2000, 3: 4000, 4: 8000}
	for attemptsMade, want := range cases {
		if got := calculateBackoff(exp, attemptsMade); got != want {
			t.Errorf("exponential backoff attemptsMade=%d = %d, want %d", attemptsMade, got, want)
		}
	}

	if got := calculateBackoff(nil, 1); got != 0 {
		t.Errorf("nil backoff = %d, want 0", got)
	}
	// Unknown strategy without a custom one -> -1 means "move straight to failed".
	if got := calculateBackoff(map[string]any{"type": "weird"}, 1); got != -1 {
		t.Errorf("unknown backoff = %d, want -1", got)
	}
}
