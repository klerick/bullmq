package bullmq

import "math"

// BackoffOptions configures retry backoff. Type is "fixed" or "exponential";
// custom strategies are out of the day-one slice.
type BackoffOptions struct {
	Type  string
	Delay int64 // milliseconds
}

// normalizeBackoff turns typed backoff options into the stored map form
// ({type, delay}), mirroring python Backoffs.normalize. A plain integer backoff
// (fixed delay) is modelled by the caller as BackoffOptions{Type:"fixed"}.
func normalizeBackoff(b *BackoffOptions) map[string]any {
	if b == nil {
		return nil
	}
	return map[string]any{"type": b.Type, "delay": b.Delay}
}

// calculateBackoff returns the retry delay in milliseconds for the given attempt,
// mirroring python Backoffs.builtin_strategies. It returns -1 for an unknown
// strategy, which the caller treats as "move straight to failed".
func calculateBackoff(backoff map[string]any, attemptsMade int) int64 {
	if backoff == nil {
		return 0
	}
	delay := toInt64(backoff["delay"])
	switch backoff["type"] {
	case "fixed":
		return delay
	case "exponential":
		return int64(math.Round(math.Pow(2, float64(attemptsMade-1)) * float64(delay)))
	default:
		return -1
	}
}
