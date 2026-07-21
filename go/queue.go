package bullmq

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Queue adds jobs to a named queue. Construct it with NewQueue and functional
// options (WithClient for DI, WithRedisOptions to build a client, WithPrefix).
type Queue struct {
	name    string
	prefix  string
	conn    *connection
	scripts *scripts
	keys    QueueKeys
}

// NewQueue creates a queue. The name must not be empty or contain ':'.
func NewQueue(name string, opts ...Option) (*Queue, error) {
	if err := ValidateQueueName(name); err != nil {
		return nil, err
	}
	cfg := newConfig(opts...)
	conn, err := newConnectionFromConfig(cfg)
	if err != nil {
		return nil, err
	}
	q := &Queue{
		name:   name,
		prefix: cfg.prefix,
		conn:   conn,
		keys:   NewQueueKeys(name, cfg.prefix),
	}
	q.scripts = newScripts(conn, cfg.prefix, name)
	return q, nil
}

// Add enqueues a job and returns it with its assigned id. A nil opts is allowed.
func (q *Queue) Add(ctx context.Context, name string, data any, opts *JobOptions) (*Job, error) {
	job := newJob(q, name, data, opts)
	id, err := q.scripts.addJob(ctx, job)
	if err != nil {
		return nil, err
	}
	job.ID = id
	return job, nil
}

// countTypes is the default set of states counted by GetJobCounts.
var countTypes = []string{"waiting", "active", "completed", "failed", "delayed", "prioritized", "paused", "waiting-children"}

// GetJob loads a job by id, or returns nil if it does not exist.
func (q *Queue) GetJob(ctx context.Context, id string) (*Job, error) {
	raw, err := q.conn.client.HGetAll(ctx, q.keys.JobKey(id)).Result()
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	return jobFromRaw(q, raw, id), nil
}

// GetJobCounts returns the number of jobs in each given state, keyed by the state
// name passed in. With no types it counts all standard states.
func (q *Queue) GetJobCounts(ctx context.Context, types ...string) (map[string]int64, error) {
	if len(types) == 0 {
		types = countTypes
	}
	counts, err := q.scripts.getCounts(ctx, types)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(types))
	for i, t := range types {
		if i < len(counts) {
			out[t] = counts[i]
		}
	}
	return out, nil
}

func (q *Queue) singleCount(ctx context.Context, t string) (int64, error) {
	counts, err := q.scripts.getCounts(ctx, []string{t})
	if err != nil || len(counts) == 0 {
		return 0, err
	}
	return counts[0], nil
}

// Per-state counts.
func (q *Queue) GetWaitingCount(ctx context.Context) (int64, error) {
	return q.singleCount(ctx, "waiting")
}
func (q *Queue) GetActiveCount(ctx context.Context) (int64, error) {
	return q.singleCount(ctx, "active")
}
func (q *Queue) GetCompletedCount(ctx context.Context) (int64, error) {
	return q.singleCount(ctx, "completed")
}
func (q *Queue) GetFailedCount(ctx context.Context) (int64, error) {
	return q.singleCount(ctx, "failed")
}
func (q *Queue) GetDelayedCount(ctx context.Context) (int64, error) {
	return q.singleCount(ctx, "delayed")
}
func (q *Queue) GetPrioritizedCount(ctx context.Context) (int64, error) {
	return q.singleCount(ctx, "prioritized")
}

// GetJobState returns a job's current state.
func (q *Queue) GetJobState(ctx context.Context, id string) (string, error) {
	return q.scripts.getState(ctx, id)
}

// GetJobs returns jobs in the given states within [start, end].
func (q *Queue) GetJobs(ctx context.Context, types []string, start, end int64, asc bool) ([]*Job, error) {
	ranges, err := q.scripts.getRanges(ctx, types, start, end, asc)
	if err != nil {
		return nil, err
	}
	var jobs []*Job
	for _, ids := range ranges {
		for _, id := range ids {
			job, err := q.GetJob(ctx, id)
			if err != nil {
				return nil, err
			}
			if job != nil {
				jobs = append(jobs, job)
			}
		}
	}
	return jobs, nil
}

// Per-state job listers.
func (q *Queue) GetWaiting(ctx context.Context, start, end int64) ([]*Job, error) {
	return q.GetJobs(ctx, []string{"waiting"}, start, end, false)
}
func (q *Queue) GetActive(ctx context.Context, start, end int64) ([]*Job, error) {
	return q.GetJobs(ctx, []string{"active"}, start, end, false)
}
func (q *Queue) GetCompleted(ctx context.Context, start, end int64) ([]*Job, error) {
	return q.GetJobs(ctx, []string{"completed"}, start, end, false)
}
func (q *Queue) GetFailed(ctx context.Context, start, end int64) ([]*Job, error) {
	return q.GetJobs(ctx, []string{"failed"}, start, end, false)
}
func (q *Queue) GetDelayed(ctx context.Context, start, end int64) ([]*Job, error) {
	return q.GetJobs(ctx, []string{"delayed"}, start, end, false)
}

// GetCountsPerPriority returns the job count for each given priority.
func (q *Queue) GetCountsPerPriority(ctx context.Context, priorities []int) ([]int64, error) {
	return q.scripts.getCountsPerPriority(ctx, priorities)
}

// IsMaxed reports whether the queue has reached its concurrency/limit ceiling.
func (q *Queue) IsMaxed(ctx context.Context) (bool, error) {
	return q.scripts.isMaxed(ctx)
}

// GetMeta returns the queue's meta hash.
func (q *Queue) GetMeta(ctx context.Context) (map[string]string, error) {
	return q.conn.client.HGetAll(ctx, q.keys.Meta()).Result()
}

// Pause stops the queue from handing out new jobs (wait -> paused).
func (q *Queue) Pause(ctx context.Context) error { return q.scripts.pause(ctx, true) }

// Resume undoes Pause (paused -> wait).
func (q *Queue) Resume(ctx context.Context) error { return q.scripts.pause(ctx, false) }

// IsPaused reports whether the queue is paused.
func (q *Queue) IsPaused(ctx context.Context) (bool, error) {
	return q.conn.client.HExists(ctx, q.keys.Meta(), "paused").Result()
}

// Drain removes waiting and prioritized jobs (and delayed if includeDelayed).
// Active, completed and failed jobs are left untouched.
func (q *Queue) Drain(ctx context.Context, includeDelayed bool) error {
	return q.scripts.drain(ctx, includeDelayed)
}

// Clean removes jobs older than grace (ms) from a state set, up to limit
// (0 = unlimited). Returns the removed job ids.
func (q *Queue) Clean(ctx context.Context, grace, limit int64, state string) ([]string, error) {
	return q.scripts.cleanJobsInSet(ctx, state, grace, limit)
}

// RetryJobs moves failed (or completed) jobs back to wait, in batches of count.
func (q *Queue) RetryJobs(ctx context.Context, state string, count int) error {
	if state == "" {
		state = "failed"
	}
	for {
		more, err := q.scripts.moveJobsToWait(ctx, state, count, nowMillis())
		if err != nil {
			return err
		}
		if more == 0 {
			return nil
		}
	}
}

// PromoteJobs moves delayed jobs to wait, in batches of count.
func (q *Queue) PromoteJobs(ctx context.Context, count int) error {
	for {
		// A far-future timestamp promotes all delayed jobs regardless of their time.
		more, err := q.scripts.moveJobsToWait(ctx, "delayed", count, 1<<62)
		if err != nil {
			return err
		}
		if more == 0 {
			return nil
		}
	}
}

// Obliterate pauses the queue and removes it entirely. With force it also removes
// active jobs.
func (q *Queue) Obliterate(ctx context.Context, force bool) error {
	if err := q.Pause(ctx); err != nil {
		return err
	}
	for {
		r, err := q.scripts.obliterate(ctx, 1000, force)
		if err != nil {
			return err
		}
		switch r {
		case -1:
			return fmt.Errorf("%w: cannot obliterate a non-paused queue", ErrInvalidConfig)
		case -2:
			return fmt.Errorf("%w: cannot obliterate a queue with active jobs (use force)", ErrInvalidConfig)
		case 0:
			return nil
		}
	}
}

// Remove deletes a job (and its children unless removeChildren is false). Returns
// true if the job was removed.
func (q *Queue) Remove(ctx context.Context, id string, removeChildren bool) (bool, error) {
	r, err := q.scripts.removeJob(ctx, id, removeChildren)
	return r == 1, err
}

// TrimEvents caps the events stream to maxLen entries.
func (q *Queue) TrimEvents(ctx context.Context, maxLen int64) (int64, error) {
	return q.conn.client.XTrimMaxLen(ctx, q.keys.Events(), maxLen).Result()
}

// GetVersion returns the queue's stored library version, if any.
func (q *Queue) GetVersion(ctx context.Context) (string, error) {
	v, err := q.conn.client.HGet(ctx, q.keys.Meta(), "version").Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}

// BulkJob describes one job for AddBulk.
type BulkJob struct {
	Name string
	Data any
	Opts *JobOptions
}

// AddBulk enqueues many jobs in a single pipeline and returns them with ids.
func (q *Queue) AddBulk(ctx context.Context, specs []BulkJob) ([]*Job, error) {
	if err := q.conn.LoadScripts(ctx); err != nil {
		return nil, err
	}
	jobs := make([]*Job, len(specs))
	cmds := make([]*redis.Cmd, len(specs))
	_, err := q.conn.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, spec := range specs {
			job := newJob(q, spec.Name, spec.Data, spec.Opts)
			jobs[i] = job
			cmd, e := q.scripts.enqueueAddJob(ctx, pipe, job)
			if e != nil {
				return e
			}
			cmds[i] = cmd
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for i, cmd := range cmds {
		res, e := cmd.Result()
		if e != nil {
			return nil, e
		}
		id, e := parseAddResult(res, jobs[i].ParentKey)
		if e != nil {
			return nil, e
		}
		jobs[i].ID = id
	}
	return jobs, nil
}

// Name returns the queue name.
func (q *Queue) Name() string { return q.name }

// Close releases resources, closing only clients the queue owns (see connection.Close).
func (q *Queue) Close() error { return q.conn.Close() }
