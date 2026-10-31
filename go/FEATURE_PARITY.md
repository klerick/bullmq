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

That feature is worth watching rather than filing away. Upstream describes it as "a messaging
layer between the nodes of a fleet" and "the first step towards reliable cross-node job
cancellation and worker control": nodes register with a lease and heartbeat, each gets a durable
inbox (a stream per node), subscriptions and topic matching live in the datastore, and dead
nodes are swept by their peers. Cancelling a running job or steering a worker needs a channel to
the _process_, which no Lua script can provide — so once `Worker`/`Job` start using it upstream,
it stops being a separate subsystem and becomes a parity item. It is experimental there and no
other port (rust, python, php, elixir, dotnet) has touched it yet; those two facts are the
trigger to revisit.

## Legend

- ✅ Implemented
- 🚧 Partial
- ❌ Not implemented

---

## Classes

| Class        | Status   | Notes                                                                     |
| ------------ | -------- | ------------------------------------------------------------------------- |
| Queue        | ✅       | add, getters, counts, lifecycle, schedulers, rate limit, dedup, defaults  |
| Worker       | ✅       | concurrency, stalled detection, lock renewal, retry/backoff, local events |
| Job          | ✅       | full lifecycle, flows, logs, progress, state                              |
| FlowProducer | ✅       | parent/child trees with cascade, per-queue default job options            |
| JobScheduler | ✅       | interval (`Every`) and cron (`Pattern`); methods on `Queue`               |
| QueueEvents  | ✅       | cross-process stream listener (`queue_events.go`)                         |
| Sandbox      | ❌ (N/A) | use native goroutines                                                     |

---

## Queue methods

| Method                                                                               | Status |
| ------------------------------------------------------------------------------------ | ------ |
| Add / AddBulk                                                                        | ✅     |
| WithDefaultJobOptions / DefaultJobOptions                                            | ✅     |
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

| Feature                                            | Status   |
| -------------------------------------------------- | -------- |
| Configurable concurrency                           | ✅       |
| Stalled job detection + recovery                   | ✅       |
| Lock renewal                                       | ✅       |
| Retry with backoff (fixed / exponential / jitter)  | ✅       |
| Rate limiting (WithLimiter)                        | ✅       |
| WorkerName / SkipStalledCheck / SkipLockRenewal    | ✅       |
| MaxStalledCount, 0 included (fail, do not recover) | ✅       |
| Job scheduler next-iteration production            | ✅       |
| Deferred failure (failParentOnFailure cascade)     | ✅       |
| Local lifecycle events (WithWorkerListener)        | ✅       |
| Cross-process events (via QueueEvents)             | ✅       |
| Telemetry / OpenTelemetry (via bullmq/otel)        | ✅       |
| Custom backoff strategies (WithBackoffStrategy)    | ✅       |
| Sandboxed processors                               | ❌ (N/A) |

### Worker events

`WithWorkerListener(WorkerListener{...})` ports upstream's `WorkerListener`
(`worker.ts:59`) as a struct of optional callbacks: `OnActive`, `OnCompleted`,
`OnFailed`, `OnError`, with upstream's arguments plus a leading `context.Context`
(the job's process context, so a handler can nest a span under `process`). They are
local to the process that finalised the job — unlike `QueueEvents`, nothing is lost
across a restart and nothing is delivered twice when several workers share a queue.

A callback runs synchronously in the job's goroutine, which matches Node
(`EventEmitter.emit` calls listeners inline) and python
(`python/bullmq/event_emitter.py`); rust is the outlier, pushing a `WorkerEvent` enum
into an unbounded channel. Because it is inline, `Close`'s drain awaits it, which is
what makes the callbacks usable for side effects during a rollout.

`OnFailed` fires on **every** failed attempt, as upstream does; `Job.FinishedOn`
separates the final failure from the ones before it (it is set only in
`moveToFailed`'s non-retry branch, `job.ts:816`).

Three deliberate choices, none of them wire-visible:

- **`active` is reported from the job's goroutine**, not from the fetch path where
  upstream emits it (`worker.ts:1040`). A listener runs inline, and inline in the
  fetch loop one slow handler would stall fetching for every other job. The state
  reported is identical.
- **A panic in a callback is recovered** and reported to `OnError` instead of taking
  the process down. Go has no equivalent of a thrown-away rejected promise, and a
  consumer's side-effect handler has no business crashing a worker.
- **A finalised job leaves the lock-renewal set before its listener runs**, matching
  upstream's `untrackJob` ahead of each handler (`worker.ts:1083-1143`), and the
  removal is guarded by the run's own lock token. Without the guard a retried job —
  put back in `wait` by `moveToFailed` and picked up again by the same worker while
  the previous run is still reporting it — would have its fresh registration deleted
  by the previous run, losing lock renewal and being run a third time.
- **`OnError` also surfaces what the port used to swallow**: a finalisation that
  could not be written to Redis, a failed lock renewal, a failed stalled check.
  Upstream reports these through `emit('error')` from `retryIfFailed`
  (`worker.ts:1593`). Its retry-the-finalisation loop itself is **not ported yet**:
  the port reports the error and lets the stalled check recover the job.

Events upstream has and the port does not: `drained`, `progress`, `stalled`, `ready`,
`closing`/`closed`, `paused`/`resumed` (the port has no `Worker.Pause`),
`lockRenewalFailed`/`locksRenewed`. Adding a field to `WorkerListener` is additive,
so these cost nothing to grow into.

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

### Default job options

`WithDefaultJobOptions` on a `Queue` (upstream `defaultJobOptions`, `queue.ts:189`) and
`FlowOpts.QueuesOptions` on `FlowProducer.Add` (`flow-producer.ts:235`, keyed by queue
name and applied at any depth) are both ported, merged by the exported
`MergeJobOptions`. Applied in `Add`, `AddBulk` and `UpsertJobScheduler` (into the stored
template, `queue.ts:497`), read back by `Queue.DefaultJobOptions()`.

Upstream merges two objects with a spread, where an absent key and a key set to zero are
distinguishable. A Go struct cannot tell them apart, so the translation has rules, and two
of them are deliberate deviations:

- a zero scalar (`Delay`, `Priority`, `Attempts`, `Timestamp`, `Lifo`, `SizeLimit`,
  `KeepLogs`) means "not set" and inherits. A default cannot be overridden back to
  `0`/`false`; `Extra` is the escape hatch, being written into the stored options last;
- `nil` pointers and `nil` `any` fields inherit, so `RemoveOnComplete: false` is a value
  and does override a default `true`, while a non-nil pointer replaces the default whole
  rather than field by field (as spreading a nested object does);
- **deviation 1 — the four parent-failure flags are replaced as a group.** If the call
  picks a policy, the default's policy is dropped, fields and `Extra` keys alike. TS and
  python merge the keys and then throw (`job.ts:1333-1355`, `job.py:66-77`); they can
  afford to, because there a default is switched off by passing the flag as `false`, which
  in Go is indistinguishable from "not set". This rule stands in for that.
- **deviation 2 — a flow node never takes `Parent` from the defaults.** Upstream overwrites
  the merged `parent` with the tree's one unconditionally (`flow-producer.ts:430`), so the
  link stays structural; a `Parent` in a queue's defaults would otherwise make a root
  somebody's child. `Queue.Add` does inherit a default `Parent`, which is what the spread
  does there.
- `JobID` comes from the call only, as upstream re-assigns `jobId` after the spread.

The flow merge runs as a pre-pass over the tree (`flowPlan`), not inside `addNode`: the
deduplication limits are checked before the transaction and must see the merged options,
and the caller's `FlowJob` must not be mutated (python guards the same invariant,
`flow_producer.py:103-107`; rust can mutate because it owns a clone,
`flow_producer.rs:1111-1116`).

`WithDefaultJobOptions` is a `Queue` option. `Worker` and `FlowProducer` accept the same
option slice but have no defaults of their own in any port, so both ignore it — for a
worker that also means the next scheduler iteration it writes is not merged a second time,
which upstream achieves by calling the job scheduler directly (`worker.ts:1002`).

Every iteration of a scheduler carries them, not just the first. The options of a produced
iteration _are_ the template for the next one: the worker hands the whole of the fetched
job's options back to the scheduler, as upstream does (`worker.ts:1000-1011`, spread in
`getNextJobOpts`), and only the per-iteration keys (`jobId`, `delay`, `timestamp`,
`prevMillis`, `repeatJobKey`, `repeat`) are overwritten. There is no field list to keep in
step with new options — the one that existed carried `attempts` and `priority` only, which
is how a schedule silently lost its `removeOn*` and its `backoff` from the second
iteration on (the trace context is the one thing deliberately not inherited — see the
divergence register). Nothing in the scheduler's
stored hash is consulted for this: `updateJobScheduler-12.lua` takes the iteration's
options from `ARGV[4]` as given, so completeness is entirely the client's job. One
consequence: an upgrade does not heal a schedule already running on a lossy chain, since
options travel job→job — re-declaring the scheduler (`UpsertJobScheduler`, which most
applications do at boot) replaces both the hash and the pending iteration.

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
`failParentOnFailure` cascade never reaches a processor, so a `failed` event is the
only place an application learns the tree's outcome. (Since `WithWorkerListener`,
`OnFailed` is the better route for that outcome — it fires in-process and carries
the job itself; the stream remains the answer when the observer is not the worker.) The events stream carries no
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

## Divergence register

Every place the port behaves differently from upstream on purpose, plus everything
porting turned up that is wrong in upstream or in a sibling port. This is the record —
nothing here is filed as an upstream issue, so it lives where the next person reading
this port will look. The reasoning sits next to each feature in the sections above; these
are the entries, in one list, with the test that pins each one. Bugs where the _port_ was
simply wrong about upstream are not here: those are in git history.

### Deliberate deviations

1. **A zero scalar means "not set" when options are merged.** Upstream spreads two
   objects, where an absent key and a key set to zero differ; a Go struct has no absent.
   So `Attempts: 0` inherits a default `3`, and `Extra` (written last) is the escape
   hatch. Pointer and `any` fields do have an absent value, so `RemoveOnComplete: false`
   overrides. — `TestMergeJobOptionsZeroDoesNotOverrideButExtraDoes`
2. **The four parent-failure flags are replaced as a group.** Upstream merges the keys and
   then throws on a combination (`job.ts:1333-1355`, `job.py:66-77`), which it can afford
   because there a default is switched off by passing `false`. — `TestMergeJobOptionsReplacesParentFailureAsAGroup`
3. **A flow node never takes `Parent` from a queue's defaults.** Upstream overwrites the
   merged `parent` with the tree's one unconditionally (`flow-producer.ts:430`); a
   `Parent` in defaults would otherwise make a root somebody's child. `Queue.Add` does
   inherit one, as the spread there does. — `TestFlowAddQueuesOptionsNeverProvideParent`
4. **A scheduler iteration does not inherit the previous one's trace context.** Upstream
   prefers an existing `opts.telemetry.metadata` over the producing span's
   (`job-scheduler.ts:220-232`) and the worker passes the fetched job's options whole, so
   every iteration of a schedule becomes a child of the first one's trace — for a 30s
   schedule, one unbounded trace. Each iteration is traced by the worker that produced it
   instead. Cost: a `tm` pinned through `Extra` applies to the iteration that declared it,
   not to later ones. — `TestSchedulerTickDoesNotInheritTraceContext`
5. **Flow nodes declared without options still propagate trace context.** Upstream guards
   the injection with `&& opts` (`flow-producer.ts:394`), which no other producing path
   does; see upstream bug 1. — `TestInteropFlowTelemetryMetadata`
6. **The failure re-stamp of `tm` is not ported.** Upstream writes it to a hash field no
   reader looks at; see upstream bug 2. The port skips the dead write.
7. **`telemetry.omitContext` is not ported.** `JobOptions.Extra["tm"]` set explicitly
   overrides what a job propagates, since an explicit value always wins.
8. **Deduplication below a flow root is refused** (`ErrDeduplicationNotOnRoot`), and
   `Replace` anywhere in a flow (`ErrDeduplicationReplaceInFlow`, which upstream types
   out). The first is upstream bug 4. — `TestFlowAddRejectsDeduplicationFromQueuesOptions`
   and the dedup flow tests
9. **A deduplicated flow add reports what it actually wrote**: the holder's id and no
   children, because a deduplicated root stores nothing. Upstream leaves the children in
   the returned tree carrying ids that name nothing; see upstream bug 5.
10. **`WithTelemetry` on `NewQueueEvents` is accepted and ignored.** Upstream makes it a
    compile error (`QueueEventsOptions extends Omit<QueueBaseOptions, 'telemetry'>`),
    which Go's one shared option slice cannot express. The listener opens no spans. —
    `TestQueueEventsTelemetryOptionIsInert`
11. **The Lua scripts are committed**, unlike in `rust/` and `python/`. A Go module is
    served straight from a git tag, so anything untracked never reaches the consumer.
12. **One extension beyond upstream:** `Job.TelemetryMetadata()` and
    `Queue.GetJobTelemetryMetadata()` expose the trace context a job carries, so a process
    reacting to a queue _event_ can continue the job's trace. Upstream has no equivalent
    and cannot grow one incidentally (see the telemetry section).

### Known divergences, left as they are

- **An interval scheduler stores a client-side guess in `opts.prevMillis`.** For `every`
  the Lua recomputes the next millis from the repeat zset, so the stored value can name
  the current iteration rather than the next; upstream stores none at all there (its
  `nextMillis` stays undefined unless a cron pattern is set). Nothing reads it: not the
  scripts, not the port.
- **`opts.prevMillis` is not used as the reference when computing a cron next-millis.**
  Upstream does `now = max(now, prevMillis)` (`job-scheduler.ts:186-188`) and rust uses
  `prev_millis`; the port computes from `now`. They differ only for an iteration fetched
  before its scheduled millisecond.

### Upstream bugs found while porting

All still present in 6.4.1.

1. **Flow nodes declared without `opts` lose the trace context.** `addNode` attaches the
   propagation metadata only when `node.opts` exists (`flow-producer.ts:394`), so
   `{ name, queueName, data }` stores no `tm` and its consumer opens a new root trace.
   `queue.ts:324`, `queue.ts:409` and `job-scheduler.ts:196` all inject regardless.
   Dropping `&& opts` looks like the whole fix.
2. **The telemetry metadata written on failure is never read back.** `Job.moveToFailed`
   HMSETs `tm` as a top-level hash field, but every reader takes `tm` out of the stored
   opts JSON (`optsFromJSON`; `raw2jobData` has no `raw.tm`). A retried job continues from
   the original producer span. Either map it back or stop writing it.
3. **Every iteration of a schedule shares one trace, forever.** The worker hands the
   fetched job's options to the scheduler whole (`worker.ts:1000-1011`) and the injection
   keeps an existing `opts.telemetry.metadata` (`job-scheduler.ts:220-232`), so `tm`
   propagates from iteration to iteration. A 30s schedule produces one trace that never
   ends; a sampler or backend that holds a trace's spans will hold them all.
4. **Deduplication on a non-root flow node silently corrupts the parent.** On a hit the add
   scripts return before linking the existing job into the new parent's dependencies, so
   the parent waits forever on a child that never joins it — while the add reports success.
5. **A deduplicated flow root returns children ids that name nothing.** Nothing below the
   root is created, but the returned tree still carries generated ids; upstream only ever
   inspects the root's result, so it does not notice.

### Found in sibling ports

Read as spec, so where they are wrong matters.

- **rust loses most job options on every scheduler iteration** — the same class of bug
  this port had: `worker.rs::pack_update_scheduler_opts` hand-picks `repeat`,
  `repeatJobKey`, `attempts`, `backoff`, `removeOnComplete`, `removeOnFail` and
  `priority`, dropping `keepLogs`, `deduplication`, `lifo`, the parent-failure flags and
  anything else.
- **rust does not validate the exclusivity of the parent-failure flags**, which TS
  (`job.ts:1333-1355`) and python (`job.py:66-77`) do. Enabling two there is accepted and
  resolved by whatever the Lua reads first.

## Intentionally out of scope

- **Sandboxed processors** — not applicable in Go; run native goroutines.
- **Job.WaitUntilFinished** — a testing convenience prone to misuse in production.
