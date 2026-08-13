# BullMQ Go — Feature Parity Tracker

Compared against Node.js BullMQ. The Go port shares the same Redis key schema and
the same include-resolved Lua scripts (embedded from `rawScripts`), so a Go
producer/consumer interoperates with any other BullMQ port. Interop is verified by
cross-runtime tests against Node BullMQ (see `interop_test.go`, `cron_interop_test.go`).

**Scripts are synced from upstream BullMQ v6** (6.0.10); interop tests run against
`bullmq@6.0.10`. v6 dropped the separate `paused` list — pausing now only sets the
meta flag and deletes the marker, leaving jobs in `wait`. A queue shared with a
pre-v6 runtime therefore disagrees about where paused jobs live; `Resume` migrates
whatever a v5 runtime left in the legacy list back into `wait`.

## Legend

- ✅ Implemented
- 🚧 Partial
- ❌ Not implemented

---

## Classes

| Class        | Status   | Notes                                                          |
| ------------ | -------- | -------------------------------------------------------------- |
| Queue        | ✅       | add, getters, counts, lifecycle, schedulers, rate limit, dedup |
| Worker       | ✅       | concurrency, stalled detection, lock renewal, retry/backoff    |
| Job          | ✅       | full lifecycle, flows, logs, progress, state                   |
| FlowProducer | ✅       | parent/child trees with cascade                                |
| JobScheduler | ✅       | interval (`Every`) and cron (`Pattern`); methods on `Queue`    |
| QueueEvents  | ✅       | cross-process stream listener (`queue_events.go`)              |
| Sandbox      | ❌ (N/A) | use native goroutines                                          |

---

## Queue methods

| Method                                                                               | Status |
| ------------------------------------------------------------------------------------ | ------ |
| Add / AddBulk                                                                        | ✅     |
| Pause / Resume / IsPaused                                                            | ✅     |
| Drain / Obliterate / Clean                                                           | ✅     |
| RetryJobs / PromoteJobs                                                              | ✅     |
| GetJob / GetJobs / per-state getters (waiting/active/completed/failed/delayed)       | ✅     |
| GetJobCounts / per-state counts / GetCountsPerPriority                               | ✅     |
| GetJobState / IsMaxed / GetMeta / GetVersion                                         | ✅     |
| Remove / TrimEvents                                                                  | ✅     |
| SetGlobalConcurrency / GetGlobalConcurrency / RemoveGlobalConcurrency                | ✅     |
| SetGlobalRateLimit / GetGlobalRateLimit / RemoveGlobalRateLimit                      | ✅     |
| RateLimit / RemoveRateLimitKey / GetRateLimitTtl                                     | ✅     |
| UpsertJobScheduler / RemoveJobScheduler / GetJobScheduler(s) / GetJobSchedulersCount | ✅     |
| GetDeduplicationJobID / RemoveDeduplicationKey (+ deprecated debounce aliases)       | ✅     |
| GetWorkers / GetWorkersCount                                                         | ✅     |
| GetMetrics (raw time series)                                                         | ✅     |
| ExportPrometheusMetrics (build a collector from GetMetrics/GetJobCounts instead)     | ❌     |
| RemoveOrphanedJobs / legacy repeatable API                                           | ❌     |

## Worker features

| Feature                                           | Status   |
| ------------------------------------------------- | -------- |
| Configurable concurrency                          | ✅       |
| Stalled job detection + recovery                  | ✅       |
| Lock renewal                                      | ✅       |
| Retry with backoff (fixed / exponential / jitter) | ✅       |
| Rate limiting (WithLimiter)                       | ✅       |
| WorkerName / SkipStalledCheck / SkipLockRenewal   | ✅       |
| Job scheduler next-iteration production           | ✅       |
| Deferred failure (failParentOnFailure cascade)    | ✅       |
| Events (via QueueEvents)                          | ✅       |
| Telemetry / OpenTelemetry (via bullmq/otel)       | ✅       |
| Custom backoff strategies (WithBackoffStrategy)   | ✅       |
| Sandboxed processors                              | ❌ (N/A) |

## Job features

| Feature                                                                                  | Status |
| ---------------------------------------------------------------------------------------- | ------ |
| UpdateProgress / UpdateData / Log / GetLogs                                              | ✅     |
| GetState / IsCompleted / IsFailed / IsActive / IsWaiting / IsDelayed / IsWaitingChildren | ✅     |
| MoveToCompleted / MoveToFailed (full retry) / MoveToWaitingChildren                      | ✅     |
| Promote / ChangeDelay / ChangePriority / Retry / Discard / Remove                        | ✅     |
| GetChildrenValues / GetDependenciesCount                                                 | ✅     |
| sizeLimit                                                                                | ✅     |

## Job options

delay ✅ · priority ✅ · attempts ✅ · backoff (fixed/exponential/jitter) ✅ · lifo ✅ ·
jobId ✅ · removeOnComplete/removeOnFail ✅ · deduplication ✅ · repeat/cron ✅ · parent ✅ ·
failParentOnFailure / removeDependencyOnFailure / ignoreDependencyOnFailure /
continueParentOnFailure ✅ · keepLogs ✅ · sizeLimit ✅ · telemetry ✅ (trace propagation via tm)

The four parent-failure options are mutually exclusive (as in python): enabling more
than one on the same job returns `ErrExclusiveParentOptions`. With none of them set,
a failed child keeps blocking its parent forever — the upstream default.

Trace context is injected on every producing path — `Queue.Add`, `Queue.AddBulk`,
`FlowProducer.Add` (per node, as upstream's addFlow/addNode do) and job schedulers.
Upstream's `telemetry.omitContext` opt-out is **not** ported: set `JobOptions.Extra["tm"]`
explicitly to override what a job propagates (an explicit value always wins).

Spans ported: `add` / `addBulk` / `addFlow` / `addNode` / `upsertJobScheduler` (producer),
`process` (consumer), `complete` and `fail` / `retry` / `delay` (internal, nested in
`process`, named by outcome as upstream's `getSpanOperation`). **Not ported yet:** the
administrative internal spans around queue and worker lifecycle (`pause`, `resume`,
`close`, `clean`, `drain`, `obliterate`, `retryJobs`, `promoteJobs`, `removeJob`,
`getNextJob`, `rateLimit`, `moveStalledJobsToWait`). They observe operations, not jobs,
so a job's trace is unaffected.

Two deliberate deviations from upstream's telemetry, both verified against Node:

- **Flow nodes without options still propagate.** `flow-producer.ts:394` guards the
  injection with `&& opts`, so a node given no options stores no `tm` and its consumer
  opens a _new root trace_. No other producing path in upstream does this
  (`queue.ts:324`, `queue.ts:409`, `job-scheduler.ts:196` all inject regardless), so the
  port treats it as an upstream slip and injects for every node — otherwise the
  api→queue trace breaks for the most common flow shape. Pinned by
  `TestInteropFlowTelemetryMetadata`, which compares both runtimes on one Redis.
- **The retry re-stamp is not ported.** `job.ts:741-752` writes the failure span's
  context into the job's `tm` _hash field_, but every reader takes `tm` out of the
  **opts JSON** (`redis-queue-backend.ts:2915-2935`; `raw2jobData` has no `raw.tm`), so
  the value is never read back — in Node too, a retried job's next attempt continues
  from the original producer span. The port skips the dead write rather than mirroring
  it byte for byte.

## Connection & infrastructure

| Feature                                         | Status        |
| ----------------------------------------------- | ------------- |
| Injectable client (DI) via WithClient           | ✅            |
| Build from options via WithRedisOptions         | ✅            |
| Dedicated blocking connection (duplicate)       | ✅            |
| Ownership-aware Close                           | ✅            |
| TLS (`rediss://` via go-redis TLSConfig)        | ✅            |
| Redis Cluster (duplicate handles ClusterClient) | 🚧 (untested) |
| Sentinel                                        | ❌            |

## Interop

Verified against Node BullMQ (pinned to the rawScripts version) in both directions:
add ↔ read, produce ↔ consume, flow cascade, cron next-times (vs `cron-parser`),
and QueueEvents.

## Intentionally out of scope

- **Sandboxed processors** — not applicable in Go; run native goroutines.
- **Job.WaitUntilFinished** — a testing convenience prone to misuse in production.
