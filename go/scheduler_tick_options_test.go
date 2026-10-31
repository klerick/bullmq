package bullmq

import (
	"reflect"
	"testing"
)

// The options of a produced iteration are the template for the next one:
// everything carries over, because upstream hands the whole of job.opts to the
// scheduler (worker.ts:1000-1011, python/bullmq/worker.py:346) and spreads it
// (job-scheduler.ts:318-357). A hand-written field list is what rotted before.
func TestSchedulerTickTemplateCarriesEveryOption(t *testing.T) {
	// Numbers arrive as float64: the map is parsed from the stored opts JSON.
	opts := map[string]any{
		"attempts":            float64(3),
		"priority":            float64(5),
		"backoff":             map[string]any{"type": "exponential", "delay": float64(1000)},
		"removeOnComplete":    true,
		"removeOnFail":        map[string]any{"count": float64(10)},
		"keepLogs":            float64(20),
		"deduplication":       map[string]any{"id": "dedup-1"},
		"failParentOnFailure": true,
		"lifo":                true,
		"sizeLimit":           float64(4096),
		"unknownToThisPort":   "kept",
	}

	tmpl := schedulerTickTemplate(opts)

	for key, want := range opts {
		got, ok := tmpl[key]
		if !ok {
			t.Errorf("template lost %q", key)
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("template[%q] = %#v, want %#v", key, got, want)
		}
	}
}

// Deviation from upstream: the trace context is not inherited. Upstream prefers an
// existing opts.telemetry.metadata over the producing span's
// (job-scheduler.ts:220-232), and injectTM has the same precedence, so passing `tm`
// on would chain every iteration of a schedule into the first one's trace forever.
func TestSchedulerTickTemplateDropsTraceContext(t *testing.T) {
	tmpl := schedulerTickTemplate(map[string]any{"attempts": float64(1), "tm": "TRACE-CTX"})

	if _, ok := tmpl["tm"]; ok {
		t.Errorf("template kept tm = %#v, want it dropped", tmpl["tm"])
	}
	if got := toInt64(tmpl["attempts"]); got != 1 {
		t.Errorf("template attempts = %d, want 1", got)
	}
}

// The worker reads job.opts after scheduling the next iteration — the process span
// takes its parent from there (worker.go) — so the template must be a copy.
func TestSchedulerTickTemplateDoesNotMutateItsArgument(t *testing.T) {
	opts := map[string]any{"attempts": float64(2), "tm": "TRACE-CTX"}

	tmpl := schedulerTickTemplate(opts)
	tmpl["attempts"] = float64(99)
	tmpl["jobId"] = "repeat:x:1"

	if got := toStr(opts["tm"]); got != "TRACE-CTX" {
		t.Errorf("source tm = %q, want it untouched", got)
	}
	if got := toInt64(opts["attempts"]); got != 2 {
		t.Errorf("source attempts = %d, want 2", got)
	}
	if _, ok := opts["jobId"]; ok {
		t.Errorf("source gained jobId = %#v", opts["jobId"])
	}
}
