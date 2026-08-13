package bullmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ParentOptions links a job to its parent in a flow. Queue is the parent's
// qualified queue key ("{prefix}:{queueName}").
type ParentOptions struct {
	ID    string
	Queue string
}

// DeduplicationOptions configures job de-duplication.
type DeduplicationOptions struct {
	ID string
}

// JobOptions configures a job at add time. Zero values mean "unset" and fall back
// to BullMQ defaults. Extra is an escape hatch for options not yet modelled.
type JobOptions struct {
	JobID            string
	Delay            int64 // milliseconds
	Priority         int
	Attempts         int
	Backoff          *BackoffOptions
	Timestamp        int64 // milliseconds; 0 means "now"
	Lifo             bool
	SizeLimit        int // max job-data size in bytes (0 = unlimited)
	RemoveOnComplete any // bool | int | map{count,age}
	RemoveOnFail     any
	Parent           *ParentOptions
	Deduplication    *DeduplicationOptions
	KeepLogs         int // max log lines kept per job (0 = unlimited)

	// Parent-failure policy: what happens to the parent when this job fails for
	// good. At most one may be set — combining them is a config error
	// (ErrExclusiveParentOptions). Without any of them the failed child stays in
	// the parent's dependencies and the parent waits forever, which is the
	// upstream default.
	FailParentOnFailure       bool // parent fails too
	ContinueParentOnFailure   bool // parent starts as soon as any child fails
	IgnoreDependencyOnFailure bool // parent stops waiting for this child; failure recorded
	RemoveDependencyOnFailure bool // parent stops waiting for this child; nothing recorded

	Extra map[string]any
}

// parentFailureOption is one of the four mutually exclusive parent-failure
// policies. The enabled one is written into the child's parent record under its
// short key: that record (hash field "parent") is the only place the Lua scripts
// read the policy from (moveToFinished-14.lua:460-492) — the copy kept in opts is
// stored for parity with Node/python but is never consulted by the scripts.
type parentFailureOption struct {
	long  string
	short string
	field func(*JobOptions) bool
}

// parentFailureOptions lists the policies in the order python/bullmq/job.py:66-72
// uses, so a rejected combination is reported identically across ports.
var parentFailureOptions = []parentFailureOption{
	{"removeDependencyOnFailure", "rdof", func(o *JobOptions) bool { return o.RemoveDependencyOnFailure }},
	{"failParentOnFailure", "fpof", func(o *JobOptions) bool { return o.FailParentOnFailure }},
	{"continueParentOnFailure", "cpof", func(o *JobOptions) bool { return o.ContinueParentOnFailure }},
	{"ignoreDependencyOnFailure", "idof", func(o *JobOptions) bool { return o.IgnoreDependencyOnFailure }},
}

// enabled reports whether the policy is on, either through its typed field or
// through Extra (long or short key) — the escape hatch consumers reached for
// before the fields existed keeps working rather than silently doing nothing.
func (p parentFailureOption) enabled(o *JobOptions) bool {
	if p.field(o) {
		return true
	}
	if b, _ := o.Extra[p.long].(bool); b {
		return true
	}
	b, _ := o.Extra[p.short].(bool)
	return b
}

// resolveParentFailureOptions returns the enabled policies, rejecting any
// combination of them. Ported from python/bullmq/job.py:66-77; validation runs
// even without a parent, so a bad combo is reported where it was written.
func resolveParentFailureOptions(o *JobOptions) ([]parentFailureOption, error) {
	var enabled []parentFailureOption
	for _, p := range parentFailureOptions {
		if p.enabled(o) {
			enabled = append(enabled, p)
		}
	}
	if len(enabled) > 1 {
		names := make([]string, len(enabled))
		for i, p := range enabled {
			names[i] = p.long
		}
		return nil, fmt.Errorf("%w: %s", ErrExclusiveParentOptions, strings.Join(names, ", "))
	}
	return enabled, nil
}

// Job is a unit of work in a queue. It is created by Queue.Add / FlowProducer, or
// reconstructed from Redis by a Worker, and passed to the processor.
type Job struct {
	ID              string
	Name            string
	Data            any
	Timestamp       int64
	Delay           int64
	Priority        int
	Attempts        int
	AttemptsMade    int
	AttemptsStarted int
	Parent          map[string]any // {id, queueKey} or nil
	ParentKey       string
	DeduplicationID string
	RepeatJobKey    string
	FailedReason    string
	// DeferredFailure is set by the failParentOnFailure cascade: Lua moves the
	// parent back to wait and records why here, leaving the actual failing to the
	// worker that picks it up (src/classes/worker.ts:951-972).
	DeferredFailure string
	ReturnValue     any
	Progress        any

	opts      map[string]any // effective options (long keys)
	sizeLimit int
	discarded bool
	token     string
	queue     *Queue
}

// newJob builds a Job from user input, applying defaults and computing the options
// map that gets packed and stored. It fails on mutually exclusive parent-failure
// options, the only user input it validates.
func newJob(queue *Queue, name string, data any, o *JobOptions) (*Job, error) {
	if o == nil {
		o = &JobOptions{}
	}
	failurePolicy, err := resolveParentFailureOptions(o)
	if err != nil {
		return nil, err
	}
	opts := buildOptsMap(o, failurePolicy)

	timestamp := o.Timestamp
	if timestamp == 0 {
		timestamp = nowMillis()
	}
	j := &Job{
		ID:        o.JobID,
		Name:      name,
		Data:      data,
		Timestamp: timestamp,
		Delay:     o.Delay,
		Priority:  o.Priority,
		Attempts:  o.Attempts,
		opts:      opts,
		sizeLimit: o.SizeLimit,
		queue:     queue,
	}
	if o.Parent != nil {
		j.ParentKey = o.Parent.Queue + ":" + o.Parent.ID // python get_parent_key
		j.Parent = map[string]any{"id": o.Parent.ID, "queueKey": o.Parent.Queue}
		// The policy travels inside the parent record, where the Lua cascade reads
		// it (src/classes/job.ts:216-234).
		for _, p := range failurePolicy {
			j.Parent[p.short] = true
		}
	}
	if o.Deduplication != nil {
		j.DeduplicationID = o.Deduplication.ID
	}
	return j, nil
}

// jobFromRaw reconstructs a Job from the flat hash returned by moveToActive,
// mirroring python Job.fromJSON. Short option keys are decoded back to long form.
func jobFromRaw(queue *Queue, raw map[string]string, jobID string) *Job {
	var data any
	_ = json.Unmarshal([]byte(orString(raw["data"], "{}")), &data)

	opts := map[string]any{}
	_ = json.Unmarshal([]byte(orString(raw["opts"], "{}")), &opts)
	opts = decodeOpts(opts)

	j := &Job{
		ID:              jobID,
		Name:            raw["name"],
		Data:            data,
		Timestamp:       toInt64(raw["timestamp"]),
		Delay:           toInt64(raw["delay"]),
		Priority:        int(toInt64(raw["priority"])),
		Attempts:        int(toInt64(opts["attempts"])),
		AttemptsStarted: int(toInt64(raw["ats"])),
		FailedReason:    raw["failedReason"],
		DeferredFailure: raw["defa"],
		ParentKey:       raw["parentKey"],
		RepeatJobKey:    raw["rjk"],
		opts:            opts,
		queue:           queue,
	}
	if v, ok := raw["attemptsMade"]; ok {
		j.AttemptsMade = int(toInt64(v))
	} else {
		j.AttemptsMade = int(toInt64(raw["atm"]))
	}
	if p, ok := raw["parent"]; ok {
		var pm map[string]any
		if json.Unmarshal([]byte(p), &pm) == nil {
			j.Parent = pm
		}
	}
	return j
}

// buildOptsMap turns typed JobOptions into the effective options map (long keys).
// failurePolicy is the already-validated parent-failure policy, stored here as
// upstream does (encodeOpts shortens it at pack time) so it round-trips.
func buildOptsMap(o *JobOptions, failurePolicy []parentFailureOption) map[string]any {
	opts := map[string]any{
		"attempts": o.Attempts,
		"delay":    o.Delay,
	}
	if o.Priority != 0 {
		opts["priority"] = o.Priority
	}
	if o.Backoff != nil {
		opts["backoff"] = normalizeBackoff(o.Backoff)
	}
	if o.Lifo {
		opts["lifo"] = true
	}
	if o.RemoveOnComplete != nil {
		opts["removeOnComplete"] = o.RemoveOnComplete
	}
	if o.RemoveOnFail != nil {
		opts["removeOnFail"] = o.RemoveOnFail
	}
	if o.Deduplication != nil {
		opts["deduplication"] = map[string]any{"id": o.Deduplication.ID}
	}
	if o.KeepLogs > 0 {
		opts["keepLogs"] = o.KeepLogs
	}
	for _, p := range failurePolicy {
		opts[p.long] = true
	}
	for k, v := range o.Extra {
		opts[k] = v
	}
	return opts
}

// optsMap returns the effective options map for packing/storing.
func (j *Job) optsMap() map[string]any { return j.opts }

func (j *Job) optBool(key string) bool {
	b, _ := j.opts[key].(bool)
	return b
}

func (j *Job) backoffMap() map[string]any {
	m, _ := j.opts["backoff"].(map[string]any)
	return m
}

// spanJobAttrs are the attributes upstream puts on a job's lifecycle spans
// (job.ts setSpanJobAttributes).
func (j *Job) spanJobAttrs() map[string]any {
	return map[string]any{"bullmq.job.name": j.Name, "bullmq.job.id": j.ID}
}

// moveToCompleted moves the job to the completed set with its return value. It
// runs inside an INTERNAL "complete" span, as upstream does (job.ts:646); called
// with the worker's process context, that span nests under the process span.
func (j *Job) moveToCompleted(ctx context.Context, returnValue any, fo finishOpts, fetchNext bool) error {
	return j.queue.tel.trace(ctx, SpanKindInternal, "complete", func(ctx context.Context, span Span) error {
		if span != nil {
			span.SetAttributes(j.spanJobAttrs())
		}
		if _, err := j.queue.scripts.moveToCompleted(ctx, j, returnValue, fo, fetchNext); err != nil {
			return err
		}
		j.ReturnValue = returnValue
		j.AttemptsMade++ // in-memory only; Redis atm is incremented by the Lua script
		return nil
	})
}

// moveToFailed applies the full retry policy, mirroring python/TS Job.moveToFailed:
// if attempts remain (and the error is recoverable), retry via backoff -> delayed,
// or immediately via retryJob; otherwise move to the failed set.
func (j *Job) moveToFailed(ctx context.Context, jobErr error, fo finishOpts, fetchNext bool) error {
	errMsg := jobErr.Error()
	j.FailedReason = errMsg

	stacktrace, _ := marshalJSON([]string{errMsg})
	fields := map[string]any{
		"failedReason": errMsg,
		"stacktrace":   string(stacktrace),
	}

	var ue *UnrecoverableError
	unrecoverable := errors.As(jobErr, &ue)

	// Decide the outcome before opening the span: its name is the outcome, exactly
	// as upstream picks it up front (job.ts:733-736 + getSpanOperation).
	retryDelay := int64(-1) // -1: no retry left, the job goes to failed
	if (j.AttemptsMade+1) < j.Attempts && !j.discarded && !unrecoverable {
		retryDelay = calculateBackoff(j.backoffMap(), j.AttemptsMade+1)
		// A non-builtin backoff type (calculateBackoff returns -1) defers to a
		// registered custom strategy, if any.
		if retryDelay == -1 && j.queue.backoffStrategy != nil {
			bt, _ := j.backoffMap()["type"].(string)
			retryDelay = j.queue.backoffStrategy(j.AttemptsMade+1, bt, jobErr, j)
		}
	}
	op := "fail"
	switch {
	case retryDelay > 0:
		op = "delay"
	case retryDelay == 0:
		op = "retry"
	}

	return j.queue.tel.trace(ctx, SpanKindInternal, op, func(ctx context.Context, span Span) error {
		if span != nil {
			span.SetAttributes(j.spanJobAttrs())
		}
		switch op {
		case "delay":
			if err := j.queue.scripts.moveToDelayed(ctx, j.ID, nowMillis(), retryDelay, fo.token, fields); err != nil {
				return err
			}
		case "retry":
			if err := j.queue.scripts.retryJob(ctx, j.ID, j.optBool("lifo"), fo.token, fields); err != nil {
				return err
			}
		default:
			if _, err := j.queue.scripts.moveToFailedFinal(ctx, j, errMsg, fo, fetchNext, fields); err != nil {
				return err
			}
		}
		j.AttemptsMade++ // in-memory only
		return nil
	})
}

// MoveToWaitingChildrenOpts configures moveToWaitingChildren. Child, when set,
// waits for that single child; otherwise the job waits for all pending children.
type MoveToWaitingChildrenOpts struct {
	Child *ParentOptions
}

// MoveToWaitingChildren moves the job to the waiting-children state. It returns
// true when the job was moved because children are still pending (the processor
// should stop and return ErrWaitingChildren), false when there were no pending
// dependencies and the processor may continue.
func (j *Job) MoveToWaitingChildren(ctx context.Context, opts MoveToWaitingChildrenOpts) (bool, error) {
	childKey := ""
	if opts.Child != nil {
		childKey = opts.Child.Queue + ":" + opts.Child.ID
	}
	return j.queue.scripts.moveToWaitingChildren(ctx, j.ID, j.token, childKey)
}

// GetChildrenValues returns the return values of this job's completed children,
// keyed by child job key. Mirrors python Job.getChildrenValues.
func (j *Job) GetChildrenValues(ctx context.Context) (map[string]any, error) {
	key := j.queue.keys.JobKey(j.ID) + ":processed"
	raw, err := j.queue.conn.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]any, len(raw))
	for k, v := range raw {
		var val any
		if json.Unmarshal([]byte(v), &val) == nil {
			out[k] = val
		} else {
			out[k] = v
		}
	}
	return out, nil
}

// GetDependenciesCount returns the number of unprocessed (pending) child dependencies.
func (j *Job) GetDependenciesCount(ctx context.Context) (int64, error) {
	counts, err := j.queue.scripts.getDependencyCounts(ctx, j.ID, []string{"unprocessed"})
	if err != nil || len(counts) == 0 {
		return 0, err
	}
	return counts[0], nil
}

// UpdateProgress stores a new progress value (any JSON-serialisable value).
func (j *Job) UpdateProgress(ctx context.Context, progress any) error {
	if err := j.queue.scripts.updateProgress(ctx, j.ID, progress); err != nil {
		return err
	}
	j.Progress = progress
	return nil
}

// UpdateData replaces the job's data.
func (j *Job) UpdateData(ctx context.Context, data any) error {
	if err := j.queue.scripts.updateData(ctx, j.ID, data); err != nil {
		return err
	}
	j.Data = data
	return nil
}

// Log appends a log line, returning the number of stored log lines. keepLogs
// (from job options) trims older lines.
func (j *Job) Log(ctx context.Context, line string) (int64, error) {
	return j.queue.scripts.addLog(ctx, j.ID, line, int(toInt64(j.opts["keepLogs"])))
}

// GetLogs returns log lines in [start, end].
func (j *Job) GetLogs(ctx context.Context, start, end int64) ([]string, error) {
	return j.queue.conn.client.LRange(ctx, j.queue.keys.JobKey(j.ID)+":logs", start, end).Result()
}

// Promote moves a delayed job to the wait state immediately.
func (j *Job) Promote(ctx context.Context) error {
	if err := j.queue.scripts.promote(ctx, j.ID); err != nil {
		return err
	}
	j.Delay = 0
	return nil
}

// ChangeDelay updates a delayed job's delay (ms).
func (j *Job) ChangeDelay(ctx context.Context, delay int64) error {
	if err := j.queue.scripts.changeDelay(ctx, j.ID, delay); err != nil {
		return err
	}
	j.Delay = delay
	return nil
}

// ChangePriority updates a job's priority (lifo controls tie-break order).
func (j *Job) ChangePriority(ctx context.Context, priority int, lifo bool) error {
	if err := j.queue.scripts.changePriority(ctx, j.ID, priority, lifo); err != nil {
		return err
	}
	j.Priority = priority
	return nil
}

// Retry moves a completed/failed job back to wait to be processed again.
func (j *Job) Retry(ctx context.Context, state string) error {
	if state == "" {
		state = "failed"
	}
	return j.queue.scripts.reprocessJob(ctx, j.ID, state, j.optBool("lifo"), false, false)
}

// Discard marks the job so it will not be retried if it fails during this run.
func (j *Job) Discard() { j.discarded = true }

// Remove deletes the job (and its children unless removeChildren is false).
func (j *Job) Remove(ctx context.Context, removeChildren bool) error {
	r, err := j.queue.scripts.removeJob(ctx, j.ID, removeChildren)
	if err != nil {
		return err
	}
	if r != 1 {
		return fmt.Errorf("bullmq: job %s could not be removed (locked or missing)", j.ID)
	}
	return nil
}

// GetState returns the job's current state.
func (j *Job) GetState(ctx context.Context) (string, error) {
	return j.queue.scripts.getState(ctx, j.ID)
}

func (j *Job) IsCompleted(ctx context.Context) (bool, error) { return j.stateIs(ctx, "completed") }
func (j *Job) IsFailed(ctx context.Context) (bool, error)    { return j.stateIs(ctx, "failed") }
func (j *Job) IsDelayed(ctx context.Context) (bool, error)   { return j.stateIs(ctx, "delayed") }
func (j *Job) IsActive(ctx context.Context) (bool, error)    { return j.stateIs(ctx, "active") }
func (j *Job) IsWaiting(ctx context.Context) (bool, error)   { return j.stateIs(ctx, "waiting") }
func (j *Job) IsWaitingChildren(ctx context.Context) (bool, error) {
	return j.stateIs(ctx, "waiting-children")
}

func (j *Job) stateIs(ctx context.Context, want string) (bool, error) {
	state, err := j.GetState(ctx)
	if err != nil {
		return false, err
	}
	return state == want, nil
}

// decodeOpts rewrites short stored option keys back to their long form,
// mirroring python job.py optsFromJSON.
func decodeOpts(opts map[string]any) map[string]any {
	out := make(map[string]any, len(opts))
	for k, v := range opts {
		if long, ok := optsDecodeMap[k]; ok {
			out[long] = v
		} else {
			out[k] = v
		}
	}
	return out
}

func nowMillis() int64 { return time.Now().UnixMilli() }

func orString(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
