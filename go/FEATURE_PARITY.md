# BullMQ Go — Feature Parity Tracker

Compared against Node.js BullMQ. The Go port shares the same Redis key schema and
the same include-resolved Lua scripts (embedded from `rawScripts`), so a Go
producer/consumer interoperates with any other BullMQ port. Interop is verified by
cross-runtime tests against Node BullMQ (see `interop_test.go`, `cron_interop_test.go`).

**Scripts are synced from upstream BullMQ v6** (6.4.1); interop tests run against
`bullmq@6.4.1`. v6 dropped the separate `paused` list — pausing now only sets the
meta flag and deletes the marker, leaving jobs in `wait`. A queue shared with a
pre-v6 runtime therefore disagrees about where paused jobs live; `Resume` migrates
whatever a v5 runtime left in the legacy list back into `wait`.

The 6.4 bump moved three scripts' key counts (`moveToDelayed` 12→11, `getCountsPerPriority`
4→2, `reprocessJob` 8→9, all shedding the `paused` key v6 no longer has — `reprocessJob` also
gained the prioritized set, which is how a retried job now keeps its priority). It also added
14 `relay*` scripts for upstream's realtime-messaging feature: they are vendored like every
other script (the embed takes the directory whole, and diverging from upstream's set costs more
than 14 extra SCRIPT LOADs in one pipelined preload) but **no bindings exist** — realtime
messaging is not ported.

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

All of upstream's deduplication options are modelled: `ID`, `TTL`, `Extend`, `Replace`,
`KeepLastIfActive`. Which branch wins is left to the scripts — only the fields the caller
set are sent, since an explicit zero is a different branch than an absent field. What
happens to an add that hits a held key depends on the holder: **waiting** → it collapses
into the queued job; **active** → it is dropped, unless `KeepLastIfActive` parks it and
promotes it once the holder finishes (keeping only the newest payload, so at most two jobs
per key exist); **delayed** → `Replace` swaps the record, and without it the add collapses.
`Replace` alone therefore does _not_ protect an update against a running job — it reaches
only a holder that has not started.

One consequence worth knowing: an add parked by `KeepLastIfActive` returns the **active**
job's id, while the parked payload later runs under an id of its own. That id is reported
by the `deduplicated` event as `deduplicatedJobId` (available through `QueueEvent.Fields`),
and `Job.DeduplicationID` is populated on jobs read back from Redis (upstream's
`raw2jobData` does the same), so a worker knows which key it holds.

Deduplication inside a flow is supported **on the root only**, and a hit is reported
honestly: `FlowProducer.Add` succeeds, returns the id of the job that already holds the
key (upstream corrects the root id the same way, `flow-producer.ts:253-264`) and returns
no children, because a deduplicated root stores nothing and the children queued behind it
in the same MULTI are never created. Upstream instead leaves those children in the
returned tree carrying ids that name nothing, since it only ever inspects the root's
result. Deduplication on a node below the root is refused up front with
`ErrDeduplicationNotOnRoot`: on a hit the add scripts return before linking the existing
job into the new parent's dependencies, so the parent would wait forever on a child that
never joins it while the add reports success (upstream has that hole). `Replace` is refused
anywhere in a flow with `ErrDeduplicationReplaceInFlow`, as upstream types it out
(`FlowDeduplicationOptions = Omit<DeduplicationOptions, 'replace'>`).

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

One deliberate **extension** beyond upstream: `Job.TelemetryMetadata()` and
`Queue.GetJobTelemetryMetadata()` expose the trace context a job carries, so a process
reacting to a queue **event** can continue the job's trace. Upstream has no equivalent
and cannot grow one incidentally — `QueueEventsOptions` extends
`Omit<QueueBaseOptions, 'telemetry'>`, so telemetry on a listener is a compile error
there, and its own OTel instrumentation patches methods, never the EventEmitter
callback that lives in user code. The need is real for flows: a parent killed by the
`failParentOnFailure` cascade never reaches a processor, so the `failed` event is the
only place an application learns the tree's outcome. The events stream carries no
trace context (no Lua script writes `tm` into it), and changing that would fork the
vendored scripts — hence resolving it from the job instead. `WithTelemetry` on
`NewQueueEvents` stays accepted-but-ignored (Go's shared options cannot express
upstream's exclusion); the listener opens no spans of its own, which
`TestQueueEventsTelemetryOptionIsInert` pins. Limit: a job removed by
`removeOnComplete`/`removeOnFail` takes its trace context with it, and the handler
falls back to a new trace.

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
