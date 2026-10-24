package bullmq

import (
	"regexp"
	"strconv"
	"testing"
)

// boundScriptKeys pins the KEYS count our bindings pass for every script the Go
// port uses. If an upstream bump changes a script's key count, its embedded
// filename changes to -N and this map mismatches, failing the build so we notice
// and update the binding. This is the primary drift guard.
var boundScriptKeys = map[string]int{
	// day-one slice
	"addStandardJob":        9,
	"addDelayedJob":         6,
	"addPrioritizedJob":     9,
	"addParentJob":          6,
	"moveToActive":          11,
	"moveToFinished":        14,
	"moveToDelayed":         11,
	"retryJob":              11,
	"moveToWaitingChildren": 7,
	"getDependencyCounts":   4,
	"removeChildDependency": 1,
	"extendLock":            2,
	"releaseLock":           1,
	"moveStalledJobsToWait": 9,
	// queue read / lifecycle / job API / schedulers / metrics (waves 6-12), which
	// used to sit outside this guard: the v6.4 bump changed reprocessJob and
	// getCountsPerPriority and nothing noticed, so every binding is pinned now.
	"getCounts":            1,
	"getCountsPerPriority": 2,
	"getRanges":            1,
	"getMetrics":           2,
	"isMaxed":              2,
	"pause":                7,
	"moveJobsToWait":       8,
	"cleanJobsInSet":       3,
	"obliterate":           2,
	"removeJob":            2,
	"reprocessJob":         9,
	"promote":              9,
	"changeDelay":          4,
	"changePriority":       7,
	"updateData":           1,
	"updateProgress":       3,
	"addLog":               2,
	"getRateLimitTtl":      2,
	"addJobScheduler":      11,
	"updateJobScheduler":   12,
	"getJobScheduler":      1,
	"removeJobScheduler":   3,
}

func TestBoundScriptsMatchEmbeddedKeyCounts(t *testing.T) {
	scripts, err := loadRawScripts()
	if err != nil {
		t.Fatalf("loadRawScripts: %v", err)
	}
	for name, want := range boundScriptKeys {
		s, ok := scripts[name]
		if !ok {
			t.Errorf("bound script %q is not embedded (renamed or removed upstream?)", name)
			continue
		}
		if s.numKeys != want {
			t.Errorf("script %q KEYS drift: embedded -%d, binding expects -%d", name, s.numKeys, want)
		}
	}
}

// Every embedded script must not reference a KEYS index beyond the count declared
// in its filename — a mismatch means the -N suffix drifted from the script body.
func TestEmbeddedScriptsKeyIndexInBounds(t *testing.T) {
	scripts, err := loadRawScripts()
	if err != nil {
		t.Fatalf("loadRawScripts: %v", err)
	}
	re := regexp.MustCompile(`KEYS\[(\d+)\]`)
	for name, s := range scripts {
		maxIdx := 0
		for _, m := range re.FindAllStringSubmatch(s.content, -1) {
			if n, _ := strconv.Atoi(m[1]); n > maxIdx {
				maxIdx = n
			}
		}
		if maxIdx > s.numKeys {
			t.Errorf("script %q references KEYS[%d] but filename declares only -%d KEYS", name, maxIdx, s.numKeys)
		}
	}
}
