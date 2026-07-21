package bullmq

import (
	"context"
	"fmt"
	"strconv"
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

func (s *scripts) addStandardJob(ctx context.Context, job *Job) (string, error) {
	keys := []string{
		s.keys.Wait(), s.keys.Paused(), s.keys.Meta(), s.keys.ID(),
		s.keys.Completed(), s.keys.Delayed(), s.keys.Active(), s.keys.Events(), s.keys.Marker(),
	}
	return s.execAdd(ctx, "addStandardJob", keys, job)
}

func (s *scripts) addDelayedJob(ctx context.Context, job *Job) (string, error) {
	keys := []string{
		s.keys.Marker(), s.keys.Meta(), s.keys.ID(),
		s.keys.Delayed(), s.keys.Completed(), s.keys.Events(),
	}
	return s.execAdd(ctx, "addDelayedJob", keys, job)
}

func (s *scripts) addPrioritizedJob(ctx context.Context, job *Job) (string, error) {
	keys := []string{
		s.keys.Marker(), s.keys.Meta(), s.keys.ID(), s.keys.Prioritized(),
		s.keys.Delayed(), s.keys.Completed(), s.keys.Active(), s.keys.Events(), s.keys.PC(),
	}
	return s.execAdd(ctx, "addPrioritizedJob", keys, job)
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
