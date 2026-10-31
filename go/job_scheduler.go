package bullmq

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// cronNextMillis computes the next run time (ms epoch) strictly after afterMillis
// for a cron pattern, matching the client-side scheduling Node does with
// cron-parser. Supports 5-field (minute-precision) and 6-field (leading seconds)
// patterns. Interop with cron-parser is verified by cross-runtime tests.
func cronNextMillis(pattern, tz string, afterMillis int64) (int64, error) {
	// cron-parser defaults to the local timezone when none is given, so match that
	// (BullMQ cron schedules without a tz are inherently server-local).
	loc := time.Local
	if tz != "" {
		l, err := time.LoadLocation(tz)
		if err != nil {
			return 0, fmt.Errorf("%w: invalid timezone %q: %v", ErrInvalidConfig, tz, err)
		}
		loc = l
	}
	var parser cron.Parser
	if len(strings.Fields(pattern)) >= 6 {
		parser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	} else {
		parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	}
	sched, err := parser.Parse(pattern)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid cron pattern %q: %v", ErrInvalidConfig, pattern, err)
	}
	return sched.Next(time.UnixMilli(afterMillis).In(loc)).UnixMilli(), nil
}

// RepeatOptions configures a job scheduler. Exactly one of Every (interval in ms)
// or Pattern (cron) must be set. Cron next-times are computed compatibly with
// Node's cron-parser, so schedules advance identically across runtimes.
type RepeatOptions struct {
	Every       int64  // milliseconds between runs
	Pattern     string // cron expression (5-field, or 6-field with leading seconds)
	Limit       int    // max iterations (0 = unlimited)
	Offset      int64  // ms offset applied to Every
	StartDate   int64  // ms epoch; do not run before this
	EndDate     int64  // ms epoch; stop after this
	Tz          string // timezone (cron only)
	Immediately bool   // run the first iteration now
	Count       int    // current iteration count (internal)
}

// JobSchedulerJSON is the stored representation of a scheduler.
type JobSchedulerJSON struct {
	Key     string // scheduler id
	Name    string
	Next    int64 // next-run millis
	Every   int64
	Pattern string
	Tz      string
	Offset  int64
	Fields  map[string]string // raw stored fields
}

// UpsertJobScheduler creates or replaces a job scheduler and enqueues its first
// job (immediate for a plain interval, otherwise delayed), returning that job.
//
// The queue's default job options are merged into the stored template
// (queue.ts:497), so every iteration the scheduler produces carries them. The
// merge lives here rather than in upsertSchedulerTemplate because the worker, which
// schedules the next iteration from the options read back off the job, must not
// have the defaults applied a second time — upstream keeps them apart the same
// way, by calling the job scheduler directly (worker.ts:1002).
func (q *Queue) UpsertJobScheduler(ctx context.Context, id string, repeat RepeatOptions, name string, data any, opts *JobOptions) (*Job, error) {
	opts = q.jobOpts(opts)
	tmpl := map[string]any{}
	if opts != nil {
		failurePolicy, err := resolveParentFailureOptions(opts)
		if err != nil {
			return nil, err
		}
		tmpl = buildOptsMap(opts, failurePolicy)
	}
	jobID, merged, err := q.upsertSchedulerTemplate(ctx, id, repeat, name, data, tmpl, true, "")
	if err != nil || jobID == "" {
		return nil, err
	}
	job, err := newJob(q, name, data, opts)
	if err != nil {
		return nil, err
	}
	job.ID = jobID
	schedulerJobTM(job, merged)
	return job, nil
}

// schedulerTickTemplate turns the options of a produced iteration into the template
// for the next one. Every option carries over — they are the template's options,
// merged with the queue's defaults back when the scheduler was declared — which is
// what upstream does by handing the whole of job.opts to the scheduler
// (worker.ts:1000-1011, python/bullmq/worker.py:346) and spreading it
// (job-scheduler.ts:318-357). The per-iteration keys (jobId, delay, timestamp,
// prevMillis, repeatJobKey, repeat) travel along and are overwritten by the
// overlay, exactly as they are there.
//
// The one exception is `tm`, deliberately dropped: injectTM keeps an existing trace
// context rather than replacing it (upstream's injection has the same precedence,
// job-scheduler.ts:220-232), so passing it on would make every iteration of a
// schedule a child of the first one's trace, forever — for a 30s schedule, one
// unbounded trace. Each iteration is instead traced by the worker that produced it.
// The cost: a `tm` pinned through Extra applies to the iteration that declared it,
// not to the ones after it.
//
// The returned map is a copy: the worker reads job.opts again after rescheduling,
// to take its process span's parent from it.
func schedulerTickTemplate(opts map[string]any) map[string]any {
	tmpl := make(map[string]any, len(opts))
	for key, value := range opts {
		if key == "tm" {
			continue
		}
		tmpl[key] = value
	}
	return tmpl
}

// upsertSchedulerTemplate writes the scheduler and materialises its next iteration
// out of tmpl, the options every iteration carries. It is the shared core of both
// entry points — the public API, which builds tmpl from typed options, and the
// worker, which inherits it from the iteration it has just fetched — and carries
// the span for both, so every produced job propagates the trace it was produced
// under, as upstream's single traced method does.
//
// It returns the id of the produced job, empty when nothing was written (the
// iteration limit or the end date was reached, or another worker had already
// materialised this iteration), and the options it was stored with.
func (q *Queue) upsertSchedulerTemplate(ctx context.Context, id string, repeat RepeatOptions, name string, data any, tmpl map[string]any, override bool, producerID string) (_ string, _ map[string]any, err error) {
	ctx, span := q.tel.start(ctx, SpanKindProducer, "upsertJobScheduler")
	defer func() { span.finish(err) }()
	switch {
	case repeat.Pattern != "" && repeat.Every > 0:
		return "", nil, fmt.Errorf("%w: both .Pattern and .Every are set for a scheduler", ErrInvalidConfig)
	case repeat.Pattern == "" && repeat.Every == 0:
		return "", nil, fmt.Errorf("%w: either .Pattern or .Every must be set for a scheduler", ErrInvalidConfig)
	}

	now := nowMillis()
	iterationCount := 1
	if repeat.Count > 0 {
		iterationCount = repeat.Count + 1
	}
	if repeat.Limit > 0 && iterationCount > repeat.Limit {
		return "", nil, nil
	}
	if repeat.EndDate > 0 && now > repeat.EndDate {
		return "", nil, nil
	}

	// nextMillis. For a cron pattern it is computed client-side (the Lua trusts it);
	// for an interval the Lua recomputes it from the repeat zset, so the value passed
	// here is only a guess — it still reaches the stored opts as prevMillis, where
	// nothing reads it (upstream leaves it undefined for an interval entirely).
	var nextMillis int64
	if repeat.Pattern != "" {
		after := now
		if repeat.StartDate > 0 && repeat.StartDate > now {
			after = repeat.StartDate
		}
		nm, err := cronNextMillis(repeat.Pattern, repeat.Tz, after)
		if err != nil {
			return "", nil, err
		}
		nextMillis = nm
		if nextMillis < now {
			nextMillis = now
		}
	} else {
		// getNextMillis in repeat.ts: floor(now/every)*every + (immediately ? 0 : every).
		nextMillis = (now / repeat.Every) * repeat.Every
		if !repeat.Immediately {
			nextMillis += repeat.Every
		}
	}

	var offsetVal any
	var offset int64
	if repeat.Offset > 0 {
		offset = repeat.Offset
		offsetVal = repeat.Offset
	}

	jobID := fmt.Sprintf("repeat:%s:%d", id, nextMillis)
	delay := nextMillis + offset - now
	if delay < 0 {
		delay = 0
	}

	merged := map[string]any{}
	for k, v := range tmpl {
		merged[k] = v
	}
	merged["jobId"] = jobID
	merged["delay"] = delay
	merged["timestamp"] = now
	merged["prevMillis"] = nextMillis
	merged["repeatJobKey"] = id
	// The repeat map carries the scheduler config forward so the worker can
	// reconstruct it for the next iteration (mirrors Node spreading ...opts.repeat).
	repeatMap := map[string]any{"offset": offsetVal, "count": iterationCount}
	if repeat.Every > 0 {
		repeatMap["every"] = repeat.Every
	}
	if repeat.Pattern != "" {
		repeatMap["pattern"] = repeat.Pattern
	}
	if repeat.Limit > 0 {
		repeatMap["limit"] = repeat.Limit
	}
	if repeat.Tz != "" {
		repeatMap["tz"] = repeat.Tz
	}
	if repeat.StartDate > 0 {
		repeatMap["startDate"] = repeat.StartDate
	}
	if repeat.EndDate > 0 {
		repeatMap["endDate"] = repeat.EndDate
	}
	merged["repeat"] = repeatMap
	// Trace context belongs to the produced job, not to the stored template:
	// upstream puts the propagation metadata in mergedOpts only.
	q.tel.injectTM(ctx, merged)

	dataJSON, err := marshalJSON(dataOrEmpty(data))
	if err != nil {
		return "", nil, err
	}

	if override {
		// Do NOT set "every" for a cron pattern: the Lua treats a present every
		// (even 0, which is truthy in Lua) as an interval and recomputes nextMillis.
		schedulerOpts := map[string]any{"name": name, "offset": offsetVal}
		if repeat.Every > 0 {
			schedulerOpts["every"] = repeat.Every
		}
		if repeat.Pattern != "" {
			schedulerOpts["pattern"] = repeat.Pattern
		}
		if repeat.Tz != "" {
			schedulerOpts["tz"] = repeat.Tz
		}
		if repeat.Limit > 0 {
			schedulerOpts["limit"] = repeat.Limit
		}
		if repeat.StartDate > 0 {
			schedulerOpts["startDate"] = repeat.StartDate
		}
		if repeat.EndDate > 0 {
			schedulerOpts["endDate"] = repeat.EndDate
		}
		if repeat.Tz != "" {
			schedulerOpts["tz"] = repeat.Tz
		}
		jobId, _, err := q.scripts.addJobScheduler(ctx, id, nextMillis, string(dataJSON),
			encodeOpts(tmpl), schedulerOpts, encodeOpts(merged), "")
		if err != nil {
			return "", nil, err
		}
		span.setAttrs(map[string]any{"bullmq.job.scheduler.id": id, "bullmq.job.id": jobId})
		return jobId, merged, nil
	}

	jobId, err := q.scripts.updateJobSchedulerNextMillis(ctx, id, nextMillis, string(dataJSON), encodeOpts(merged), producerID)
	if err != nil {
		return "", nil, err
	}
	if jobId == "" {
		return "", nil, nil
	}
	span.setAttrs(map[string]any{"bullmq.job.scheduler.id": id, "bullmq.job.id": jobId})
	return jobId, merged, nil
}

// schedulerJobTM mirrors onto the returned handle the trace context that was
// stored with the job, so the in-memory job matches what Redis holds.
func schedulerJobTM(job *Job, merged map[string]any) {
	if tm := toStr(merged["tm"]); tm != "" {
		job.opts["tm"] = tm
	}
}

// GetJobScheduler returns a scheduler by id, or nil if it does not exist.
func (q *Queue) GetJobScheduler(ctx context.Context, id string) (*JobSchedulerJSON, error) {
	fields, next, err := q.scripts.getJobScheduler(ctx, id)
	if err != nil || fields == nil {
		return nil, err
	}
	return &JobSchedulerJSON{
		Key:     id,
		Name:    fields["name"],
		Next:    next,
		Every:   toInt64(fields["every"]),
		Pattern: fields["pattern"],
		Tz:      fields["tz"],
		Offset:  toInt64(fields["offset"]),
		Fields:  fields,
	}, nil
}

// GetJobSchedulersCount returns the number of configured schedulers.
func (q *Queue) GetJobSchedulersCount(ctx context.Context) (int64, error) {
	return q.conn.client.ZCard(ctx, q.keys.Repeat()).Result()
}

// GetJobSchedulers returns the schedulers in [start, end] of the repeat zset.
func (q *Queue) GetJobSchedulers(ctx context.Context, start, end int64) ([]*JobSchedulerJSON, error) {
	ids, err := q.conn.client.ZRange(ctx, q.keys.Repeat(), start, end).Result()
	if err != nil {
		return nil, err
	}
	out := make([]*JobSchedulerJSON, 0, len(ids))
	for _, id := range ids {
		sched, err := q.GetJobScheduler(ctx, id)
		if err != nil {
			return nil, err
		}
		if sched != nil {
			out = append(out, sched)
		}
	}
	return out, nil
}

// RemoveJobScheduler removes a scheduler (and its pending job). Returns true if it
// existed. The Lua returns 0 when it removed the scheduler, 1 when there was none.
func (q *Queue) RemoveJobScheduler(ctx context.Context, id string) (bool, error) {
	r, err := q.scripts.removeJobScheduler(ctx, id)
	return r == 0, err
}

// isJobScheduler reports whether a repeatJobKey belongs to a modern scheduler
// (not a legacy repeatable key with 5+ colon segments). Mirrors the worker check.
func isJobScheduler(repeatJobKey string) bool {
	return repeatJobKey != "" && strings.Count(repeatJobKey, ":") < 4
}

func dataOrEmpty(data any) any {
	if data == nil {
		return map[string]any{}
	}
	return data
}

// repeatFromOpts reconstructs RepeatOptions from a stored job's opts.repeat map,
// used by the worker to schedule the next iteration.
func repeatFromOpts(v any) RepeatOptions {
	m, _ := v.(map[string]any)
	if m == nil {
		return RepeatOptions{}
	}
	r := RepeatOptions{
		Every:     toInt64(m["every"]),
		Offset:    toInt64(m["offset"]),
		Count:     int(toInt64(m["count"])),
		StartDate: toInt64(m["startDate"]),
		EndDate:   toInt64(m["endDate"]),
		Limit:     int(toInt64(m["limit"])),
	}
	if p, ok := m["pattern"].(string); ok {
		r.Pattern = p
	}
	if tz, ok := m["tz"].(string); ok {
		r.Tz = tz
	}
	return r
}
