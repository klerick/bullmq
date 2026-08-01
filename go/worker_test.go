package bullmq

import (
	"context"
	"testing"
	"time"
)

// blockTimeout mirrors python/bullmq/worker.py::getBlockTimeout: it waits exactly as
// long as is left until the next delayed job (or the end of a rate-limit window),
// floored at minimumBlockTimeout, and falls back to the drain delay when nothing is
// pending. The sub-second cases are the point: rounding them up to a whole second
// delays every delayed job by up to a second.
func TestWorkerBlockTimeout(t *testing.T) {
	tests := []struct {
		name       string
		blockUntil func() int64
		drainDelay time.Duration
		min, max   time.Duration
	}{
		{
			name:       "idle worker blocks for the drain delay",
			blockUntil: func() int64 { return 0 },
			drainDelay: 5 * time.Second,
			min:        5 * time.Second,
			max:        5 * time.Second,
		},
		{
			name:       "drain delay below the floor is raised to it",
			blockUntil: func() int64 { return 0 },
			drainDelay: 0,
			min:        minimumBlockTimeout,
			max:        minimumBlockTimeout,
		},
		{
			name:       "wake-up already due blocks for the minimum, not zero",
			blockUntil: func() int64 { return nowMillis() - 500 },
			drainDelay: 5 * time.Second,
			min:        minimumBlockTimeout,
			max:        minimumBlockTimeout,
		},
		{
			name:       "sub-second wake-up keeps its fraction",
			blockUntil: func() int64 { return nowMillis() + 349 },
			drainDelay: 5 * time.Second,
			min:        250 * time.Millisecond,
			max:        349 * time.Millisecond,
		},
		{
			name:       "wake-up beyond the cap is clamped",
			blockUntil: func() int64 { return nowMillis() + 60_000 },
			drainDelay: 5 * time.Second,
			min:        maxBlockTimeout,
			max:        maxBlockTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{drainDelay: tt.drainDelay, blockUntil: tt.blockUntil()}
			got := w.blockTimeout()
			if got < tt.min || got > tt.max {
				t.Errorf("blockTimeout() = %s, want within [%s, %s]", got, tt.min, tt.max)
			}
		})
	}
}

// newBZPopMinCmd must pass the timeout as fractional seconds. go-redis's typed
// BZPopMin runs it through formatSec, which rounds anything below one second up to 1s
// and logs a warning; Redis >= 6 takes the fraction as-is, so building the command
// directly keeps the precision upstream relies on and keeps the log quiet.
func TestNewBZPopMinCmdCarriesFractionalTimeout(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		want    float64
	}{
		{name: "sub-second timeout stays a fraction", timeout: 349 * time.Millisecond, want: 0.349},
		{name: "floor timeout stays a fraction", timeout: time.Millisecond, want: 0.001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newBZPopMinCmd(context.Background(), "bull:test:marker", tt.timeout)

			args := cmd.Args()
			if len(args) != 3 {
				t.Fatalf("args = %v, want [bzpopmin key timeout]", args)
			}
			if args[0] != "bzpopmin" {
				t.Errorf("args[0] = %v, want bzpopmin", args[0])
			}
			if args[1] != "bull:test:marker" {
				t.Errorf("args[1] = %v, want the marker key", args[1])
			}
			got, ok := args[2].(float64)
			if !ok {
				t.Fatalf("timeout arg is %T (%v), want float64 — a whole-second arg means formatSec truncated it", args[2], args[2])
			}
			if got != tt.want {
				t.Errorf("timeout arg = %v, want %v", got, tt.want)
			}
		})
	}
}
