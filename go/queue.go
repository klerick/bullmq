package bullmq

import "context"

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

// Name returns the queue name.
func (q *Queue) Name() string { return q.name }

// Close releases resources, closing only clients the queue owns (see connection.Close).
func (q *Queue) Close() error { return q.conn.Close() }
