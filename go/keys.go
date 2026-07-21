package bullmq

import (
	"encoding/base64"
	"fmt"
	"strings"
)

// DefaultPrefix is the default key prefix, compatible with Node.js BullMQ.
const DefaultPrefix = "bull"

// ValidateQueueName validates a queue name using the same rule as Node.js BullMQ:
// the name must be non-empty and must not contain the ':' separator.
func ValidateQueueName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: queue name must be provided", ErrInvalidConfig)
	}
	if strings.Contains(name, ":") {
		return fmt.Errorf("%w: queue name cannot contain ':'", ErrInvalidConfig)
	}
	return nil
}

// ResolveParentQueueKey resolves ParentOptions.queue into a qualified queue key.
//
// It accepts either an unqualified name (`queue`) or an already-qualified key for the
// current prefix (`prefix:queue`). The queue-name portion is always validated, so
// malformed values like `foo:bar` are rejected unless they are a valid
// `{prefix}:{queueName}` pair for the current prefix.
func ResolveParentQueueKey(prefix, queue string) (string, error) {
	qualified := prefix + ":"
	if name, ok := strings.CutPrefix(queue, qualified); ok {
		if err := ValidateQueueName(name); err != nil {
			return "", err
		}
		return queue, nil
	}
	if err := ValidateQueueName(queue); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s", prefix, queue), nil
}

// QueueKeys holds the prefix and queue name needed to generate all Redis keys.
// Every key follows the pattern {prefix}:{name}:{suffix}.
type QueueKeys struct {
	prefix string
	name   string
}

// NewQueueKeys builds a key context. An empty prefix falls back to DefaultPrefix ("bull").
func NewQueueKeys(name, prefix string) QueueKeys {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	return QueueKeys{prefix: prefix, name: name}
}

// Prefix returns the prefix.
func (k QueueKeys) Prefix() string { return k.prefix }

// Name returns the queue name.
func (k QueueKeys) Name() string { return k.name }

// Base is the base key without a suffix: {prefix}:{name}.
func (k QueueKeys) Base() string { return k.prefix + ":" + k.name }

// KeyPrefix is the prefix for job keys: {prefix}:{name}: (with a trailing colon).
// This is the same value the python bindings pass as self.keys[""] (ARGV[1] of add scripts).
func (k QueueKeys) KeyPrefix() string { return k.Base() + ":" }

// JobKey builds a job-specific key: {prefix}:{name}:{jobID}.
func (k QueueKeys) JobKey(jobID string) string { return k.Base() + ":" + jobID }

// Get is a dynamic lookup by suffix: {prefix}:{name}:{suffix}.
func (k QueueKeys) Get(suffix string) string { return k.Base() + ":" + suffix }

// QualifiedName is {prefix}:{name} (used for parent.queueKey and the worker client name).
func (k QueueKeys) QualifiedName() string { return k.Base() }

// Well-known keys (mirror of rust/src/keys.rs) ─────────────────────────────
func (k QueueKeys) Wait() string            { return k.Get("wait") }
func (k QueueKeys) Active() string          { return k.Get("active") }
func (k QueueKeys) Paused() string          { return k.Get("paused") }
func (k QueueKeys) Delayed() string         { return k.Get("delayed") }
func (k QueueKeys) Prioritized() string     { return k.Get("prioritized") }
func (k QueueKeys) Completed() string       { return k.Get("completed") }
func (k QueueKeys) Failed() string          { return k.Get("failed") }
func (k QueueKeys) WaitingChildren() string { return k.Get("waiting-children") }
func (k QueueKeys) Stalled() string         { return k.Get("stalled") }
func (k QueueKeys) StalledCheck() string    { return k.Get("stalled-check") }
func (k QueueKeys) Limiter() string         { return k.Get("limiter") }
func (k QueueKeys) Events() string          { return k.Get("events") }
func (k QueueKeys) Meta() string            { return k.Get("meta") }
func (k QueueKeys) Marker() string          { return k.Get("marker") }
func (k QueueKeys) PC() string              { return k.Get("pc") }
func (k QueueKeys) ID() string              { return k.Get("id") }
func (k QueueKeys) Repeat() string          { return k.Get("repeat") }

// ClientName builds the worker's Redis client name in the Node.js format
// {prefix}:{base64(queueName)}{suffix}, so getWorkers can discover clients across runtimes.
func (k QueueKeys) ClientName(suffix string) string {
	return fmt.Sprintf("%s:%s%s", k.prefix, nameToBase64(k.name), suffix)
}

// nameToBase64 encodes a queue name as standard padded base64, matching
// Buffer.from(name).toString('base64') in Node.js.
func nameToBase64(name string) string {
	return base64.StdEncoding.EncodeToString([]byte(name))
}
