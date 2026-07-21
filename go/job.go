package bullmq

import "time"

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
// to BullMQ defaults. Fields grow across waves; Extra is an escape hatch for options
// not yet modelled as typed fields.
type JobOptions struct {
	JobID         string
	Delay         int64 // milliseconds
	Priority      int
	Attempts      int
	Timestamp     int64 // milliseconds; 0 means "now"
	Parent        *ParentOptions
	Deduplication *DeduplicationOptions
	Extra         map[string]any
}

// Job is a unit of work in a queue. It is created by Queue.Add / FlowProducer and
// passed to a Worker's processor.
type Job struct {
	ID              string
	Name            string
	Data            any
	Timestamp       int64
	Delay           int64
	Priority        int
	Attempts        int
	Parent          map[string]any // {id, queueKey} or nil
	ParentKey       string
	DeduplicationID string
	RepeatJobKey    string

	opts  *JobOptions
	queue *Queue
}

// newJob builds a Job from user input, applying defaults (timestamp, parent key).
func newJob(queue *Queue, name string, data any, opts *JobOptions) *Job {
	if opts == nil {
		opts = &JobOptions{}
	}
	timestamp := opts.Timestamp
	if timestamp == 0 {
		timestamp = nowMillis()
	}
	j := &Job{
		ID:        opts.JobID,
		Name:      name,
		Data:      data,
		Timestamp: timestamp,
		Delay:     opts.Delay,
		Priority:  opts.Priority,
		Attempts:  opts.Attempts,
		opts:      opts,
		queue:     queue,
	}
	if opts.Parent != nil {
		// python get_parent_key: "{queue}:{id}"
		j.ParentKey = opts.Parent.Queue + ":" + opts.Parent.ID
		j.Parent = map[string]any{"id": opts.Parent.ID, "queueKey": opts.Parent.Queue}
	}
	if opts.Deduplication != nil {
		j.DeduplicationID = opts.Deduplication.ID
	}
	return j
}

// optsMap builds the options map that gets packed and stored. It uses long keys
// (deduplication) — encodeOpts shortens known ones (-> "de") at pack time. Mirrors
// the RedisJobOptions that Node stores: attempts and delay are always present.
func (j *Job) optsMap() map[string]any {
	m := make(map[string]any)
	m["attempts"] = j.Attempts
	m["delay"] = j.Delay
	if j.Priority != 0 {
		m["priority"] = j.Priority
	}
	if j.DeduplicationID != "" {
		m["deduplication"] = map[string]any{"id": j.DeduplicationID}
	}
	for k, v := range j.opts.Extra {
		m[k] = v
	}
	return m
}

// nowMillis returns the current Unix time in milliseconds, matching the JS/Python
// Date.now() granularity BullMQ uses for timestamps.
func nowMillis() int64 {
	return time.Now().UnixMilli()
}
