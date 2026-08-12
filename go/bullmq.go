// Package bullmq is a Go port of BullMQ, wire-compatible with the Node.js, Python,
// Rust and PHP implementations. It shares the same Redis key schema and the same
// include-resolved Lua scripts (embedded from rawScripts), so a Go producer/consumer
// can interoperate with queues driven by any other BullMQ port.
package bullmq

// Version is the Go port's own version. Ports are versioned independently of the
// TypeScript package; this tracks the Go module, tagged as go/vX.Y.Z.
const Version = "0.2.0"
