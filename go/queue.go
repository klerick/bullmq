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

// Name returns the queue name.
func (q *Queue) Name() string { return q.name }

// Close releases resources, closing only clients the queue owns (see connection.Close).
func (q *Queue) Close() error { return q.conn.Close() }
