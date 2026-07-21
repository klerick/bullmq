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
	"addStandardJob":        9,
	"addDelayedJob":         6,
	"addPrioritizedJob":     9,
	"addParentJob":          6,
	"moveToActive":          11,
	"moveToFinished":        14,
	"moveToDelayed":         12,
	"retryJob":              11,
	"moveToWaitingChildren": 7,
	"getDependencyCounts":   4,
	"removeChildDependency": 1,
	"extendLock":            2,
	"releaseLock":           1,
	"moveStalledJobsToWait": 9,
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
