package bullmq

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Event names emitted on the queue events stream (mirroring Node's names).
const (
	EventAdded            = "added"
	EventWaiting          = "waiting"
	EventActive           = "active"
	EventCompleted        = "completed"
	EventFailed           = "failed"
	EventDelayed          = "delayed"
	EventProgress         = "progress"
	EventStalled          = "stalled"
	EventRemoved          = "removed"
	EventDrained          = "drained"
	EventPaused           = "paused"
	EventResumed          = "resumed"
	EventWaitingChildren  = "waiting-children"
	EventRetriesExhausted = "retries-exhausted"
	EventCleaned          = "cleaned"
	EventDeduplicated     = "deduplicated"
)

// QueueEvent is one entry from the queue events stream. It is observable from any
// process connected to the same Redis, unlike a worker's in-process callbacks.
type QueueEvent struct {
	StreamID string            // XREAD entry id (usable to resume)
	Event    string            // event name
	JobID    string            // job id, when the event has one
	Fields   map[string]string // all raw fields
}

// ReturnValue returns the completed job's return value (JSON), if present.
func (e QueueEvent) ReturnValue() string { return e.Fields["returnvalue"] }

// FailedReason returns the failed job's reason, if present.
func (e QueueEvent) FailedReason() string { return e.Fields["failedReason"] }

// QueueEvents listens to a queue's events stream (`{prefix}:{name}:events`) via a
// dedicated blocking connection. Construct with the same options as Queue, except
// WithTelemetry, which is accepted but ignored (see NewQueueEvents).
type QueueEvents struct {
	conn         *connection
	keys         QueueKeys
	blockTimeout time.Duration
}

// NewQueueEvents creates a listener for the named queue.
//
// WithTelemetry has no effect here: a listener opens no spans of its own. Upstream
// makes that a compile error (QueueEventsOptions extends
// Omit<QueueBaseOptions, 'telemetry'>); Go's shared options cannot express the
// exclusion, so the option is accepted and ignored. An event carries no trace
// context — nothing writes `tm` into the stream — so a handler that wants to join
// the job's trace resolves it itself via Queue.GetJobTelemetryMetadata (or
// Job.TelemetryMetadata) and extracts it. Keeping that policy in the caller means
// only the events it actually handles cost a read.
func NewQueueEvents(name string, opts ...Option) (*QueueEvents, error) {
	if err := ValidateQueueName(name); err != nil {
		return nil, err
	}
	cfg := newConfig(opts...)
	conn, err := newConnectionFromConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &QueueEvents{conn: conn, keys: NewQueueKeys(name, cfg.prefix), blockTimeout: 5 * time.Second}, nil
}

// Listen starts consuming events, returning a channel of events and a channel of
// errors. Both close when ctx is cancelled. By default only events produced after
// Listen starts are delivered; pass a fromID (an earlier StreamID, or "0" for the
// whole stream) to resume from a point.
func (qe *QueueEvents) Listen(ctx context.Context, fromID ...string) (<-chan QueueEvent, <-chan error) {
	events := make(chan QueueEvent, 64)
	errs := make(chan error, 1)

	lastID := "$" // only new events by default
	if len(fromID) > 0 && fromID[0] != "" {
		lastID = fromID[0]
	}

	go func() {
		defer close(events)
		defer close(errs)
		for {
			if ctx.Err() != nil {
				return
			}
			res, err := qe.conn.blockingClient.XRead(ctx, &redis.XReadArgs{
				Streams: []string{qe.keys.Events(), lastID},
				Block:   qe.blockTimeout,
				Count:   100,
			}).Result()
			if err == redis.Nil {
				continue // block elapsed with no new events
			}
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				select {
				case errs <- err:
				default:
				}
				time.Sleep(100 * time.Millisecond) // bounded backoff on transient errors
				continue
			}
			for _, stream := range res {
				for _, msg := range stream.Messages {
					lastID = msg.ID
					select {
					case events <- parseQueueEvent(msg):
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return events, errs
}

// Close releases the listener's connection (only clients it owns).
func (qe *QueueEvents) Close() error { return qe.conn.Close() }

func parseQueueEvent(msg redis.XMessage) QueueEvent {
	fields := make(map[string]string, len(msg.Values))
	for k, v := range msg.Values {
		fields[k] = fmt.Sprint(v)
	}
	return QueueEvent{
		StreamID: msg.ID,
		Event:    fields["event"],
		JobID:    fields["jobId"],
		Fields:   fields,
	}
}
