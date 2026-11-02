package bullmq

import (
	"embed"
	"fmt"
	"strconv"
	"strings"
)

// commandsFS holds the include-resolved Lua commands, copied from rawScripts by the
// package.json `copy:lua:go` step (cpx rawScripts/*.lua -> go/commands). Unlike rust/
// and python/, which gitignore their copy and regenerate it when they release, this
// one is committed: a Go module is served straight from a git tag, so anything
// untracked never reaches the consumer (FEATURE_PARITY.md, deviation 11). The embed
// directive is its own guard — a missing copy fails the build with "pattern
// commands/*.lua: no matching files found". Otherwise this mirrors rust's include_str!.
//
//go:embed commands/*.lua
var commandsFS embed.FS

// rawScript is an embedded Lua command with its KEYS count parsed from the filename.
type rawScript struct {
	name    string
	numKeys int
	content string
}

// parseScriptName splits a command filename like "addStandardJob-9.lua" into its
// command name ("addStandardJob") and KEYS count (9). The "-N" suffix is the
// cross-language contract encoded in the filename.
func parseScriptName(filename string) (name string, numKeys int, err error) {
	base := strings.TrimSuffix(filename, ".lua")
	idx := strings.LastIndex(base, "-")
	if idx < 0 {
		return "", 0, fmt.Errorf("bullmq: script %q has no -N key-count suffix", filename)
	}
	numKeys, err = strconv.Atoi(base[idx+1:])
	if err != nil {
		return "", 0, fmt.Errorf("bullmq: script %q has invalid key count: %w", filename, err)
	}
	return base[:idx], numKeys, nil
}

// loadRawScripts reads every embedded Lua command, keyed by command name.
func loadRawScripts() (map[string]rawScript, error) {
	entries, err := commandsFS.ReadDir("commands")
	if err != nil {
		return nil, fmt.Errorf("bullmq: reading embedded commands: %w", err)
	}
	scripts := make(map[string]rawScript, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".lua") {
			continue
		}
		name, numKeys, err := parseScriptName(e.Name())
		if err != nil {
			return nil, err
		}
		content, err := commandsFS.ReadFile("commands/" + e.Name())
		if err != nil {
			return nil, fmt.Errorf("bullmq: reading %q: %w", e.Name(), err)
		}
		scripts[name] = rawScript{name: name, numKeys: numKeys, content: string(content)}
	}
	return scripts, nil
}
