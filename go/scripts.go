package bullmq

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// scripts is the bindings layer: it assembles KEYS/ARGV for each Lua command,
// runs it, and parses the reply. Ported from python/bullmq/scripts.py.
type scripts struct {
	conn      *connection
	keys      QueueKeys
	prefix    string
	queueName string
}

func newScripts(conn *connection, prefix, queueName string) *scripts {
	return &scripts{
		conn:      conn,
		keys:      NewQueueKeys(queueName, prefix),
		prefix:    prefix,
		queueName: queueName,
	}
}

// run executes a registered script by name on the main client.
func (s *scripts) run(ctx context.Context, name string, keys []string, args ...any) (any, error) {
	script, ok := s.conn.scripts[name]
	if !ok {
		return nil, fmt.Errorf("bullmq: unknown script %q", name)
	}
	return script.Run(ctx, s.conn.client, keys, args...).Result()
}

// runVoid executes a script that returns nothing. A Lua script with no return
// value yields a RESP nil, which go-redis surfaces as redis.Nil — not an error here.
func (s *scripts) runVoid(ctx context.Context, name string, keys []string, args ...any) error {
	_, err := s.run(ctx, name, keys, args...)
	if err == redis.Nil {
		return nil
	}
	return err
}

// addJobArgs builds the three ARGV values shared by all add-scripts:
// [ pack(argsArray), jsonData, pack(encodedOpts) ]. The 9-element argsArray layout
// is the cross-port contract (see addStandardJob-9.lua and TS scripts.ts::addJob).
func (s *scripts) addJobArgs(job *Job) ([]any, error) {
	jsonData, err := marshalJSON(job.Data)
	if err != nil {
		return nil, err
	}
	packedOpts, err := packMsgpack(encodeOpts(job.optsMap()))
	if err != nil {
		return nil, err
	}

	var parentKey, parentDepsKey, parent, repeatJobKey, dedupKey any
	if job.ParentKey != "" {
		parentKey = job.ParentKey
		parentDepsKey = job.ParentKey + ":dependencies"
	}
	if job.Parent != nil {
		parent = job.Parent
	}
	if job.RepeatJobKey != "" {
		repeatJobKey = job.RepeatJobKey
	}
	if job.DeduplicationID != "" {
		dedupKey = s.keys.Get("de") + ":" + job.DeduplicationID
	}

	argsArray := []any{
		s.keys.KeyPrefix(), // [1] key prefix (trailing colon)
		job.ID,             // [2] custom id, or "" to let the script INCR one
		job.Name,           // [3] name
		job.Timestamp,      // [4] timestamp
		parentKey,          // [5] parentKey?
		parentDepsKey,      // [6] parent dependencies key?
		parent,             // [7] parent {id, queueKey}?
		repeatJobKey,       // [8] repeat job key?
		dedupKey,           // [9] deduplication key?
	}
	packedArgs, err := packMsgpack(argsArray)
	if err != nil {
		return nil, err
	}
	return []any{packedArgs, string(jsonData), packedOpts}, nil
}

// addJob dispatches to the right add-script based on options, mirroring
// python scripts.py::addJob and TS scripts.ts::addJob.
func (s *scripts) addJob(ctx context.Context, job *Job) (string, error) {
	switch {
	case job.Delay > 0:
		return s.addDelayedJob(ctx, job)
	case job.Priority > 0:
		return s.addPrioritizedJob(ctx, job)
	default:
		return s.addStandardJob(ctx, job)
	}
}

// Key lists per add-script (KEYS order is the cross-port contract).
func (s *scripts) standardJobKeys() []string {
	return []string{
		s.keys.Wait(), s.keys.Paused(), s.keys.Meta(), s.keys.ID(),
		s.keys.Completed(), s.keys.Delayed(), s.keys.Active(), s.keys.Events(), s.keys.Marker(),
	}
}

func (s *scripts) delayedJobKeys() []string {
	return []string{
		s.keys.Marker(), s.keys.Meta(), s.keys.ID(),
		s.keys.Delayed(), s.keys.Completed(), s.keys.Events(),
	}
}

func (s *scripts) prioritizedJobKeys() []string {
	return []string{
		s.keys.Marker(), s.keys.Meta(), s.keys.ID(), s.keys.Prioritized(),
		s.keys.Delayed(), s.keys.Completed(), s.keys.Active(), s.keys.Events(), s.keys.PC(),
	}
}

func (s *scripts) parentJobKeys() []string {
	return []string{
		s.keys.Meta(), s.keys.ID(), s.keys.Delayed(),
		s.keys.WaitingChildren(), s.keys.Completed(), s.keys.Events(),
	}
}

// addJobScript picks the add-script name and keys for a leaf job.
func (s *scripts) addJobScript(job *Job) (string, []string) {
	switch {
	case job.Delay > 0:
		return "addDelayedJob", s.delayedJobKeys()
	case job.Priority > 0:
		return "addPrioritizedJob", s.prioritizedJobKeys()
	default:
		return "addStandardJob", s.standardJobKeys()
	}
}

func (s *scripts) addStandardJob(ctx context.Context, job *Job) (string, error) {
	return s.execAdd(ctx, "addStandardJob", s.standardJobKeys(), job)
}

func (s *scripts) addDelayedJob(ctx context.Context, job *Job) (string, error) {
	return s.execAdd(ctx, "addDelayedJob", s.delayedJobKeys(), job)
}

func (s *scripts) addPrioritizedJob(ctx context.Context, job *Job) (string, error) {
	return s.execAdd(ctx, "addPrioritizedJob", s.prioritizedJobKeys(), job)
}

func (s *scripts) execAdd(ctx context.Context, script string, keys []string, job *Job) (string, error) {
	args, err := s.addJobArgs(job)
	if err != nil {
		return "", err
	}
	res, err := s.run(ctx, script, keys, args...)
	if err != nil {
		return "", err
	}
	return parseAddResult(res, job.ParentKey)
}

// enqueueAddJob queues a leaf-job add on the given scripter (e.g. a pipeline) and
// returns the pending command; the result is read after the pipeline executes.
func (s *scripts) enqueueAddJob(ctx context.Context, scripter redis.Scripter, job *Job) (*redis.Cmd, error) {
	args, err := s.addJobArgs(job)
	if err != nil {
		return nil, err
	}
	name, keys := s.addJobScript(job)
	return s.conn.scripts[name].Run(ctx, scripter, keys, args...), nil
}

// enqueueAddParentJob queues a parent-job add (goes into waiting-children) on the
// given scripter.
func (s *scripts) enqueueAddParentJob(ctx context.Context, scripter redis.Scripter, job *Job) (*redis.Cmd, error) {
	args, err := s.addJobArgs(job)
	if err != nil {
		return nil, err
	}
	return s.conn.scripts["addParentJob"].Run(ctx, scripter, s.parentJobKeys(), args...), nil
}

// parseAddResult turns an add-script reply into a job id, or a typed error for a
// negative status code. Generated ids come back as integers, custom ids as strings.
func parseAddResult(res any, parentKey string) (string, error) {
	switch v := res.(type) {
	case int64:
		if v < 0 {
			return "", finishedError(ScriptErrorCode(v), errorContext{parentKey: parentKey, command: "addJob"})
		}
		return strconv.FormatInt(v, 10), nil
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("bullmq: unexpected addJob result %v (%T)", res, res)
	}
}

// moveToActiveOpts is the per-fetch input packed for moveToActive.
type moveToActiveOpts struct {
	token        string
	lockDuration int64
	limiter      any
}

// moveToActive fetches the next job into the active state. It returns the job's
// flat hash (or nil when none is available) plus the id and the rate-limit /
// next-delayed timestamps. KEYS/ARGV from moveToActive-11.lua.
func (s *scripts) moveToActive(ctx context.Context, o moveToActiveOpts) (jobData map[string]string, jobID string, limitUntil, delayUntil int64, err error) {
	keys := []string{
		s.keys.Wait(), s.keys.Active(), s.keys.Prioritized(), s.keys.Events(),
		s.keys.Stalled(), s.keys.Limiter(), s.keys.Delayed(), s.keys.Paused(),
		s.keys.Meta(), s.keys.PC(), s.keys.Marker(),
	}
	packedOpts, err := packMsgpack(map[string]any{
		"token": o.token, "lockDuration": o.lockDuration, "limiter": o.limiter,
	})
	if err != nil {
		return nil, "", 0, 0, err
	}
	args := []any{s.keys.KeyPrefix(), nowMillis(), packedOpts}
	res, err := s.run(ctx, "moveToActive", keys, args...)
	if err != nil {
		return nil, "", 0, 0, err
	}
	jobData, jobID, limitUntil, delayUntil = parseNextJobData(res)
	return jobData, jobID, limitUntil, delayUntil, nil
}

// parseNextJobData decodes the {jobData, jobId, limitUntil, delayUntil} reply of
// moveToActive/moveToFinished. Mirrors python scripts.py::raw2NextJobData. jobData
// is nil when no job was returned (the script sends 0 in that slot).
func parseNextJobData(res any) (map[string]string, string, int64, int64) {
	arr, ok := res.([]any)
	if !ok || len(arr) == 0 {
		return nil, "", 0, 0
	}
	var jobData map[string]string
	if flat, ok := arr[0].([]any); ok && len(flat) > 0 {
		jobData = flatArrayToMap(flat)
	}
	var jobID string
	if jobData != nil && len(arr) >= 2 {
		jobID = toStr(arr[1])
	}
	var limitUntil, delayUntil int64
	if len(arr) >= 4 {
		limitUntil = toInt64(arr[2])
		delayUntil = toInt64(arr[3])
	}
	return jobData, jobID, limitUntil, delayUntil
}

// finishOpts carries the worker-level context needed to finish a job.
type finishOpts struct {
	token            string
	lockDuration     int64
	limiter          any
	removeOnComplete any
	removeOnFail     any
}

// moveToCompleted moves the job to the completed set with a JSON-encoded return value.
func (s *scripts) moveToCompleted(ctx context.Context, job *Job, returnValue any, fo finishOpts, fetchNext bool) (any, error) {
	valBytes, err := marshalJSON(returnValue)
	if err != nil {
		return nil, err
	}
	keys, args := s.moveToFinishedArgs(job, string(valBytes), "returnvalue", "completed", fo, fo.removeOnComplete, fetchNext, nil)
	return s.runMoveToFinished(ctx, job.ID, keys, args)
}

// moveToFailedFinal moves the job to the failed set (no more retries). Unlike the
// return value, failedReason is stored raw (not JSON-encoded), matching Node/rust.
func (s *scripts) moveToFailedFinal(ctx context.Context, job *Job, failedReason string, fo finishOpts, fetchNext bool, fieldsToUpdate map[string]any) (any, error) {
	keys, args := s.moveToFinishedArgs(job, failedReason, "failedReason", "failed", fo, fo.removeOnFail, fetchNext, fieldsToUpdate)
	return s.runMoveToFinished(ctx, job.ID, keys, args)
}

// moveToFinishedArgs builds KEYS/ARGV for moveToFinished-14. Ported from python
// scripts.py::moveToFinishedArgs. value is already transformed (JSON for completed,
// raw for failed).
func (s *scripts) moveToFinishedArgs(job *Job, value, propName, target string, fo finishOpts, shouldRemove any, fetchNext bool, fieldsToUpdate map[string]any) ([]string, []any) {
	keys := []string{
		s.keys.Wait(), s.keys.Active(), s.keys.Prioritized(), s.keys.Events(),
		s.keys.Stalled(), s.keys.Limiter(), s.keys.Delayed(), s.keys.Paused(),
		s.keys.Meta(), s.keys.PC(), s.keys.Get(target),
		s.keys.JobKey(job.ID), s.keys.Get("metrics:" + target), s.keys.Marker(),
	}
	packedOpts, _ := packMsgpack(map[string]any{
		"token":          fo.token,
		"keepJobs":       getKeepJobs(shouldRemove),
		"limiter":        fo.limiter,
		"lockDuration":   fo.lockDuration,
		"attempts":       job.Attempts,
		"attemptsMade":   job.AttemptsMade,
		"maxMetricsSize": "",
		"fpof":           job.optBool("failParentOnFailure"),
		"cpof":           job.optBool("continueParentOnFailure"),
		"idof":           job.optBool("ignoreDependencyOnFailure"),
		"rdof":           job.optBool("removeDependencyOnFailure"),
	})
	fetchStr := ""
	if fetchNext {
		fetchStr = "1"
	}
	args := []any{job.ID, nowMillis(), propName, value, target, fetchStr, s.keys.KeyPrefix(), packedOpts}
	if len(fieldsToUpdate) > 0 {
		packedFields, _ := packMsgpack(objectToFlatArray(fieldsToUpdate))
		args = append(args, packedFields)
	}
	return keys, args
}

func (s *scripts) runMoveToFinished(ctx context.Context, jobID string, keys []string, args []any) (any, error) {
	res, err := s.run(ctx, "moveToFinished", keys, args...)
	if err != nil {
		return nil, err
	}
	if code, ok := res.(int64); ok && code < 0 {
		return nil, finishedError(ScriptErrorCode(code), errorContext{jobID: jobID, command: "moveToFinished", state: "active"})
	}
	return res, nil
}

// moveToDelayed moves an active job to the delayed set for a backoff retry.
// KEYS/ARGV from moveToDelayed-12.lua.
func (s *scripts) moveToDelayed(ctx context.Context, jobID string, timestamp, delay int64, token string, fieldsToUpdate map[string]any) error {
	keys := []string{
		s.keys.Marker(), s.keys.Active(), s.keys.Prioritized(), s.keys.Delayed(),
		s.keys.JobKey(jobID), s.keys.Events(), s.keys.Meta(), s.keys.Stalled(),
		s.keys.Wait(), s.keys.Limiter(), s.keys.Paused(), s.keys.PC(),
	}
	args := []any{s.keys.KeyPrefix(), strconv.FormatInt(timestamp, 10), jobID, token, delay, "0"}
	if len(fieldsToUpdate) > 0 {
		packed, _ := packMsgpack(objectToFlatArray(fieldsToUpdate))
		args = append(args, packed)
	} else {
		args = append(args, "")
	}
	args = append(args, "0") // fetchNext = false
	res, err := s.run(ctx, "moveToDelayed", keys, args...)
	if err != nil {
		return err
	}
	if code, ok := res.(int64); ok && code < 0 {
		return finishedError(ScriptErrorCode(code), errorContext{jobID: jobID, command: "moveToDelayed", state: "active"})
	}
	return nil
}

// retryJob moves an active job back to wait for an immediate retry.
// KEYS/ARGV from retryJob-11.lua.
func (s *scripts) retryJob(ctx context.Context, jobID string, lifo bool, token string, fieldsToUpdate map[string]any) error {
	keys := []string{
		s.keys.Active(), s.keys.Wait(), s.keys.Paused(), s.keys.JobKey(jobID),
		s.keys.Meta(), s.keys.Events(), s.keys.Delayed(), s.keys.Prioritized(),
		s.keys.PC(), s.keys.Marker(), s.keys.Stalled(),
	}
	pushCmd := "LPUSH"
	if lifo {
		pushCmd = "RPUSH"
	}
	args := []any{s.keys.KeyPrefix(), nowMillis(), pushCmd, jobID, token}
	if len(fieldsToUpdate) > 0 {
		packed, _ := packMsgpack(objectToFlatArray(fieldsToUpdate))
		args = append(args, packed)
	}
	res, err := s.run(ctx, "retryJob", keys, args...)
	if err != nil {
		return err
	}
	if code, ok := res.(int64); ok && code < 0 {
		return finishedError(ScriptErrorCode(code), errorContext{jobID: jobID, command: "retryJob", state: "active"})
	}
	return nil
}

// moveToWaitingChildren moves an active parent into the waiting-children state.
// Returns true if it moved (pending children remain), false if there were no
// pending dependencies (the caller may proceed). KEYS/ARGV from
// moveToWaitingChildren-7.lua. childKey is "" to wait for all children.
func (s *scripts) moveToWaitingChildren(ctx context.Context, jobID, token, childKey string) (bool, error) {
	jobKey := s.keys.JobKey(jobID)
	keys := []string{
		s.keys.Active(), s.keys.WaitingChildren(), jobKey,
		jobKey + ":dependencies", jobKey + ":unsuccessful", s.keys.Stalled(), s.keys.Events(),
	}
	args := []any{token, childKey, nowMillis(), jobID, s.keys.KeyPrefix()}
	res, err := s.run(ctx, "moveToWaitingChildren", keys, args...)
	if err != nil {
		return false, err
	}
	code := toInt64(res)
	switch {
	case code == 0:
		return true, nil // moved: pending children
	case code == 1:
		return false, nil // no pending dependencies
	case code < 0:
		return false, finishedError(ScriptErrorCode(code), errorContext{jobID: jobID, command: "moveToWaitingChildren", state: "active"})
	default:
		return false, nil
	}
}

// getDependencyCounts returns child-state counts for the given types (any of
// "processed", "unprocessed", "ignored", "failed"). From getDependencyCounts-4.lua.
func (s *scripts) getDependencyCounts(ctx context.Context, jobID string, types []string) ([]int64, error) {
	jobKey := s.keys.JobKey(jobID)
	keys := []string{
		jobKey + ":processed", jobKey + ":dependencies", jobKey + ":ignored", jobKey + ":failed",
	}
	args := make([]any, len(types))
	for i, t := range types {
		args[i] = t
	}
	res, err := s.run(ctx, "getDependencyCounts", keys, args...)
	if err != nil {
		return nil, err
	}
	arr, _ := res.([]any)
	out := make([]int64, len(arr))
	for i, v := range arr {
		out[i] = toInt64(v)
	}
	return out, nil
}

// removeChildDependency breaks the parent-child link by removing the child's
// reference from its parent. From removeChildDependency-1.lua.
func (s *scripts) removeChildDependency(ctx context.Context, jobID, parentKey string) error {
	keys := []string{s.keys.KeyPrefix()}
	args := []any{s.keys.JobKey(jobID), parentKey}
	res, err := s.run(ctx, "removeChildDependency", keys, args...)
	if err != nil {
		return err
	}
	if code := toInt64(res); code < 0 {
		return finishedError(ScriptErrorCode(code), errorContext{jobID: jobID, command: "removeChildDependency"})
	}
	return nil
}

// extendLock renews a job's lock (SET lock PX + SREM from stalled). Returns true
// if the lock was still held by token and got renewed. From extendLock-2.lua.
func (s *scripts) extendLock(ctx context.Context, jobID, token string, duration int64) (bool, error) {
	keys := []string{s.keys.JobKey(jobID) + ":lock", s.keys.Stalled()}
	args := []any{token, duration, jobID}
	res, err := s.run(ctx, "extendLock", keys, args...)
	if err != nil {
		return false, err
	}
	return toInt64(res) == 1, nil
}

// releaseLock deletes a job's lock if still held by token. From releaseLock-1.lua.
func (s *scripts) releaseLock(ctx context.Context, jobID, token string) error {
	keys := []string{s.keys.JobKey(jobID) + ":lock"}
	args := []any{token, "0"}
	_, err := s.run(ctx, "releaseLock", keys, args...)
	return err
}

// moveStalledJobsToWait detects jobs whose worker died (lock not renewed across
// two checks) and moves them back to wait (or to failed past maxStalledCount).
// It returns the ids that were moved this cycle. From moveStalledJobsToWait-9.lua.
func (s *scripts) moveStalledJobsToWait(ctx context.Context, maxStalledCount int, stalledInterval int64) ([]string, error) {
	keys := []string{
		s.keys.Stalled(), s.keys.Wait(), s.keys.Active(), s.keys.StalledCheck(),
		s.keys.Meta(), s.keys.Paused(), s.keys.Marker(), s.keys.Events(), s.keys.Repeat(),
	}
	args := []any{maxStalledCount, s.keys.KeyPrefix(), nowMillis(), stalledInterval}
	res, err := s.run(ctx, "moveStalledJobsToWait", keys, args...)
	if err != nil {
		return nil, err
	}
	arr, _ := res.([]any)
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		out = append(out, toStr(v))
	}
	return out, nil
}

// pause moves wait->paused (pause=true) or paused->wait (resume) and flips the
// meta "paused" flag. From pause-7.lua.
func (s *scripts) pause(ctx context.Context, pause bool) error {
	src, dst, arg := "wait", "paused", "paused"
	if !pause {
		src, dst, arg = "paused", "wait", "resumed"
	}
	keys := []string{
		s.keys.Get(src), s.keys.Get(dst), s.keys.Meta(), s.keys.Prioritized(),
		s.keys.Events(), s.keys.Delayed(), s.keys.Marker(),
	}
	return s.runVoid(ctx, "pause", keys, arg)
}

// drain removes waiting/prioritized (and optionally delayed) jobs. From drain-5.lua.
func (s *scripts) drain(ctx context.Context, delayed bool) error {
	keys := []string{s.keys.Wait(), s.keys.Paused(), s.keys.Delayed(), s.keys.Prioritized(), s.keys.Repeat()}
	d := "0"
	if delayed {
		d = "1"
	}
	return s.runVoid(ctx, "drain", keys, s.keys.KeyPrefix(), d)
}

// cleanJobsInSet removes jobs older than grace (ms) from a set, up to limit
// (0 = no limit). Returns the removed job ids. From cleanJobsInSet-3.lua.
func (s *scripts) cleanJobsInSet(ctx context.Context, set string, grace, limit int64) ([]string, error) {
	set = transformStateType(set)
	keys := []string{s.keys.Get(set), s.keys.Events(), s.keys.Repeat()}
	args := []any{s.keys.KeyPrefix(), nowMillis() - grace, limit, set}
	res, err := s.run(ctx, "cleanJobsInSet", keys, args...)
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	arr, _ := res.([]any)
	out := make([]string, len(arr))
	for i, v := range arr {
		out[i] = toStr(v)
	}
	return out, nil
}

// moveJobsToWait bulk-moves jobs from a source state back to wait. Used by
// retryJobs (state completed/failed) and promoteJobs (state delayed). Returns 1 if
// more remain (hit count), 0 when done. From moveJobsToWait-8.lua.
func (s *scripts) moveJobsToWait(ctx context.Context, state string, count int, timestamp int64) (int64, error) {
	keys := []string{
		s.keys.KeyPrefix(), s.keys.Events(), s.keys.Get(state), s.keys.Wait(),
		s.keys.Paused(), s.keys.Meta(), s.keys.Active(), s.keys.Marker(),
	}
	res, err := s.run(ctx, "moveJobsToWait", keys, count, timestamp, state)
	if err != nil {
		return 0, err
	}
	return toInt64(res), nil
}

// obliterate removes up to count jobs of the (paused) queue. Returns 1 if more
// remain, 0 when done, -1 if not paused, -2 if active jobs without force.
// From obliterate-2.lua.
func (s *scripts) obliterate(ctx context.Context, count int, force bool) (int64, error) {
	forceArg := ""
	if force {
		forceArg = "force"
	}
	res, err := s.run(ctx, "obliterate", []string{s.keys.Meta(), s.keys.KeyPrefix()}, count, forceArg)
	if err != nil {
		return 0, err
	}
	return toInt64(res), nil
}

// removeJob removes a job (and optionally its children). From removeJob-2.lua.
func (s *scripts) removeJob(ctx context.Context, jobID string, removeChildren bool) (int64, error) {
	rc := 0
	if removeChildren {
		rc = 1
	}
	res, err := s.run(ctx, "removeJob", []string{s.keys.JobKey(jobID), s.keys.Repeat()}, jobID, rc, s.keys.KeyPrefix())
	if err != nil {
		return 0, err
	}
	return toInt64(res), nil
}

// transformStateType maps the public "waiting" type to its Redis key suffix "wait".
func transformStateType(t string) string {
	if t == "waiting" {
		return "wait"
	}
	return t
}

// getCounts returns the job count for each requested type. From getCounts-1.lua.
func (s *scripts) getCounts(ctx context.Context, types []string) ([]int64, error) {
	keys := []string{s.keys.KeyPrefix()}
	args := make([]any, len(types))
	for i, t := range types {
		args[i] = transformStateType(t)
	}
	res, err := s.run(ctx, "getCounts", keys, args...)
	if err != nil {
		return nil, err
	}
	arr, _ := res.([]any)
	out := make([]int64, len(arr))
	for i, v := range arr {
		out[i] = toInt64(v)
	}
	return out, nil
}

// getState returns a job's state ("completed", "failed", "delayed", "prioritized",
// "active", "waiting", "waiting-children", "unknown"). Uses getStateV2 (Redis >= 6.0.6).
func (s *scripts) getState(ctx context.Context, jobID string) (string, error) {
	keys := []string{
		s.keys.Completed(), s.keys.Failed(), s.keys.Delayed(), s.keys.Active(),
		s.keys.Wait(), s.keys.Paused(), s.keys.WaitingChildren(), s.keys.Prioritized(),
	}
	args := []any{jobID, s.keys.JobKey(jobID)}
	res, err := s.run(ctx, "getStateV2", keys, args...)
	if err != nil {
		return "", err
	}
	return toStr(res), nil
}

// getRanges returns the job ids in each requested state's list/zset within
// [start, end]. From getRanges-1.lua.
func (s *scripts) getRanges(ctx context.Context, types []string, start, end int64, asc bool) ([][]string, error) {
	ascStr := "0"
	if asc {
		ascStr = "1"
	}
	args := []any{start, end, ascStr}
	for _, t := range types {
		args = append(args, transformStateType(t))
	}
	res, err := s.run(ctx, "getRanges", []string{s.keys.KeyPrefix()}, args...)
	if err != nil {
		return nil, err
	}
	arr, _ := res.([]any)
	out := make([][]string, len(arr))
	for i, e := range arr {
		ids, _ := e.([]any)
		out[i] = make([]string, len(ids))
		for j, id := range ids {
			out[i][j] = toStr(id)
		}
	}
	return out, nil
}

// getCountsPerPriority returns the job count for each requested priority.
// From getCountsPerPriority-4.lua.
func (s *scripts) getCountsPerPriority(ctx context.Context, priorities []int) ([]int64, error) {
	keys := []string{s.keys.Wait(), s.keys.Paused(), s.keys.Meta(), s.keys.Prioritized()}
	args := make([]any, len(priorities))
	for i, p := range priorities {
		args[i] = p
	}
	res, err := s.run(ctx, "getCountsPerPriority", keys, args...)
	if err != nil {
		return nil, err
	}
	arr, _ := res.([]any)
	out := make([]int64, len(arr))
	for i, v := range arr {
		out[i] = toInt64(v)
	}
	return out, nil
}

// isMaxed reports whether the queue has reached its concurrency/limit ceiling.
// From isMaxed-2.lua (Lua false comes back as nil).
func (s *scripts) isMaxed(ctx context.Context) (bool, error) {
	res, err := s.run(ctx, "isMaxed", []string{s.keys.Meta(), s.keys.Active()})
	if err == redis.Nil { // Lua `false` comes back as a RESP nil
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return toInt64(res) == 1, nil
}

// getKeepJobs normalises removeOnComplete/removeOnFail into the {count|age|...}
// map the Lua expects. Mirrors python scripts.py::getKeepJobs.
func getKeepJobs(shouldRemove any) map[string]any {
	switch v := shouldRemove.(type) {
	case map[string]any:
		return v
	case int:
		return map[string]any{"count": v}
	case int64:
		return map[string]any{"count": v}
	case bool:
		if v {
			return map[string]any{"count": 0}
		}
		return map[string]any{"count": -1}
	default:
		return map[string]any{"count": -1} // nil / unknown -> keep all
	}
}

// objectToFlatArray flattens {k:v, ...} into [k, v, ...] for packing.
// Mirrors python utils.object_to_flat_array.
func objectToFlatArray(m map[string]any) []any {
	out := make([]any, 0, len(m)*2)
	for k, v := range m {
		out = append(out, k, v)
	}
	return out
}
