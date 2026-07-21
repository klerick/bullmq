# BullMQ Go — Feature Parity Tracker

Compared against Node.js BullMQ. The Go port shares the same Redis key schema and
the same include-resolved Lua scripts (embedded from `rawScripts`), so a Go
producer/consumer interoperates with any other BullMQ port. Interop is verified by
cross-runtime tests against Node BullMQ (see `interop_test.go`, `cron_interop_test.go`).

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
| GetMetrics / ExportPrometheusMetrics                                                 | ❌     |
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
| Events (via QueueEvents)                          | ✅       |
| Custom backoff strategies / Telemetry             | ❌       |
| Sandboxed processors                              | ❌ (N/A) |

## Job features

| Feature                                                                                  | Status |
| ---------------------------------------------------------------------------------------- | ------ |
| UpdateProgress / UpdateData / Log / GetLogs                                              | ✅     |
| GetState / IsCompleted / IsFailed / IsActive / IsWaiting / IsDelayed / IsWaitingChildren | ✅     |
| MoveToCompleted / MoveToFailed (full retry) / MoveToWaitingChildren                      | ✅     |
| Promote / ChangeDelay / ChangePriority / Retry / Discard / Remove                        | ✅     |
| GetChildrenValues / GetDependenciesCount                                                 | ✅     |
| sizeLimit                                                                                | ❌     |

## Job options

delay ✅ · priority ✅ · attempts ✅ · backoff (fixed/exponential/jitter) ✅ · lifo ✅ ·
jobId ✅ · removeOnComplete/removeOnFail ✅ · deduplication ✅ · repeat/cron ✅ · parent ✅ ·
failParentOnFailure / removeDependencyOnFailure / ignoreDependencyOnFailure /
continueParentOnFailure ✅ (stored) · keepLogs 🚧 · sizeLimit ❌ · telemetry ❌

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
- **Telemetry / OpenTelemetry** — optional, behind a future build tag.
- **Job.WaitUntilFinished** — a testing convenience prone to misuse in production.
