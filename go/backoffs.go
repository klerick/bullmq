package bullmq

import (
	"math"
	"math/rand"
)

// BackoffOptions configures retry backoff. Type is "fixed" or "exponential".
// Jitter (0..1) randomises the delay down to delay*(1-jitter).
type BackoffOptions struct {
	Type   string
	Delay  int64   // milliseconds
	Jitter float64 // 0..1
}

// normalizeBackoff turns typed backoff options into the stored map form
// ({type, delay[, jitter]}), mirroring Backoffs.normalize.
func normalizeBackoff(b *BackoffOptions) map[string]any {
	if b == nil {
		return nil
	}
	m := map[string]any{"type": b.Type, "delay": b.Delay}
	if b.Jitter > 0 {
		m["jitter"] = b.Jitter
	}
	return m
}

// calculateBackoff returns the retry delay in milliseconds for the given attempt,
// mirroring Backoffs.builtinStrategies (incl. jitter). It returns -1 for an unknown
// strategy, which the caller treats as "move straight to failed".
func calculateBackoff(backoff map[string]any, attemptsMade int) int64 {
	if backoff == nil {
		return 0
	}
	delay := toInt64(backoff["delay"])
	var computed int64
	switch backoff["type"] {
	case "fixed":
		computed = delay
	case "exponential":
		computed = int64(math.Round(math.Pow(2, float64(attemptsMade-1)) * float64(delay)))
	default:
		return -1
	}
	if jitter := toFloat64(backoff["jitter"]); jitter > 0 {
		minDelay := float64(computed) * (1 - jitter)
		return int64(rand.Float64()*float64(computed)*jitter + minDelay)
	}
	return computed
}
