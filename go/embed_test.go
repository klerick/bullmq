package bullmq

import (
	"strings"
	"testing"
)

// The filename encodes the KEYS contract: "moveToFinished-14.lua" means 14 KEYS.
func TestParseScriptName(t *testing.T) {
	ok := []struct {
		file    string
		name    string
		numKeys int
	}{
		{"addStandardJob-9.lua", "addStandardJob", 9},
		{"moveToFinished-14.lua", "moveToFinished", 14},
		{"removeChildDependency-1.lua", "removeChildDependency", 1},
	}
	for _, c := range ok {
		name, numKeys, err := parseScriptName(c.file)
		if err != nil {
			t.Errorf("parseScriptName(%q) unexpected err: %v", c.file, err)
		}
		if name != c.name || numKeys != c.numKeys {
			t.Errorf("parseScriptName(%q) = (%q, %d), want (%q, %d)", c.file, name, numKeys, c.name, c.numKeys)
		}
	}
	if _, _, err := parseScriptName("noKeySuffix.lua"); err == nil {
		t.Error("parseScriptName without -N suffix should error")
	}
}

// All embedded rawScripts load, are include-resolved, and expose the right key counts.
func TestLoadRawScripts(t *testing.T) {
	scripts, err := loadRawScripts()
	if err != nil {
		t.Fatalf("loadRawScripts: %v", err)
	}
	if len(scripts) == 0 {
		t.Fatal("no embedded scripts loaded")
	}
	// Day-one scripts must be present with the expected KEYS count.
	want := map[string]int{
		"addStandardJob":        9,
		"addParentJob":          6,
		"moveToActive":          11,
		"moveToFinished":        14,
		"moveToWaitingChildren": 7,
		"getDependencyCounts":   4,
	}
	for name, numKeys := range want {
		s, ok := scripts[name]
		if !ok {
			t.Errorf("missing embedded script %q", name)
			continue
		}
		if s.numKeys != numKeys {
			t.Errorf("%s: numKeys = %d, want %d", name, s.numKeys, numKeys)
		}
		if s.content == "" {
			t.Errorf("%s: empty content", name)
		}
		// Includes must already be resolved by yarn generate:raw:scripts.
		if strings.Contains(s.content, "@include") {
			t.Errorf("%s: still contains unresolved @include", name)
		}
	}
}
