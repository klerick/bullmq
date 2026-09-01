package bullmq

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// FlowJob is a node in a job flow tree. A node with Children becomes a parent job
// (placed in waiting-children); leaves are added normally. Prefix overrides the
// producer prefix for that node's queue.
type FlowJob struct {
	Name      string
	QueueName string
	Data      any
	Opts      *JobOptions
	Children  []*FlowJob
	Prefix    string
}

// JobNode is the result of adding a flow: the created job and its children.
type JobNode struct {
	Job      *Job
	Children []*JobNode
}

// FlowProducer adds trees of parent/child jobs atomically. A parent completes only
// after all of its children have completed (the cascade is handled Redis-side by
// moveToFinished). Construct with the same options as Queue.
type FlowProducer struct {
	conn   *connection
	prefix string
	tel    Telemetry
}

// NewFlowProducer creates a flow producer.
func NewFlowProducer(opts ...Option) (*FlowProducer, error) {
	cfg := newConfig(opts...)
	conn, err := newConnectionFromConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &FlowProducer{conn: conn, prefix: cfg.prefix, tel: cfg.telemetry}, nil
}

// telFor builds the span helper for one queue. Unlike a Queue, a flow spans many
// queues, so the span-name prefix belongs to the node, not to the producer.
func (fp *FlowProducer) telFor(queueName string) telemetryHelper {
	return telemetryHelper{t: fp.tel, name: queueName}
}

// assignFlowIDs gives every node that has no explicit id one, up front. The ids
// must be decided before the first attempt: a retry has to re-add the very same
// jobs, which the add scripts recognise as duplicates (EXISTS on the job key)
// instead of enqueueing a second copy. Generating them inside addNode would make
// a retry produce a whole new tree. The caller's FlowJob is left untouched.
func assignFlowIDs(node *FlowJob, ids map[*FlowJob]string) {
	if node == nil {
		return
	}
	if node.Opts == nil || node.Opts.JobID == "" {
		ids[node] = genID()
	}
	for _, child := range node.Children {
		assignFlowIDs(child, ids)
	}
}

// Add inserts a whole flow tree in a single transaction and returns the job tree.
func (fp *FlowProducer) Add(ctx context.Context, flow *FlowJob) (*JobNode, error) {
	// The scripts must be in Redis before the transaction: inside MULTI, go-redis
	// cannot fall back from EVALSHA to EVAL, because it checks the error while the
	// command is still only queued.
	gen, err := fp.conn.scriptCache.ensure(ctx)
	if err != nil {
		return nil, err
	}

	ids := make(map[*FlowJob]string)
	assignFlowIDs(flow, ids)

	var out *JobNode
	err = fp.telFor(flow.QueueName).trace(ctx, SpanKindProducer, "addFlow", func(ctx context.Context, span Span) error {
		if span != nil {
			span.SetAttributes(map[string]any{"bullmq.queue": flow.QueueName, "bullmq.flow.name": flow.Name})
		}
		tree, e := fp.addTree(ctx, flow, ids)
		if !isNoScriptErr(e) {
			out = tree
			return e
		}
		// Redis lost the script cache (restart, failover, SCRIPT FLUSH). Reload it
		// once for every caller that noticed, then replay the transaction — with the
		// same ids, so a partially applied attempt cannot become a second tree.
		if rerr := fp.conn.scriptCache.reload(ctx, gen); rerr != nil {
			return rerr
		}
		tree, e = fp.addTree(ctx, flow, ids)
		out = tree
		return e
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// addTree runs one attempt: the whole tree in a single MULTI.
func (fp *FlowProducer) addTree(ctx context.Context, flow *FlowJob, ids map[*FlowJob]string) (*JobNode, error) {
	var tree *JobNode
	var cmds []*redis.Cmd
	if _, err := fp.conn.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		var e error
		tree, e = fp.addNode(ctx, pipe, &cmds, flow, nil, ids)
		return e
	}); err != nil {
		return nil, err
	}
	// Surface any negative status code the scripts returned inside the pipeline.
	for _, cmd := range cmds {
		res, err := cmd.Result()
		if err != nil {
			return nil, err
		}
		if code, ok := res.(int64); ok && code < 0 {
			return nil, finishedError(ScriptErrorCode(code), errorContext{command: "addJob"})
		}
	}
	return tree, nil
}

// addNode recursively adds a node and its children, threading parentOpts downward.
// Flow jobs get a generated id up-front so children can reference the parent before
// the pipeline executes.
func (fp *FlowProducer) addNode(ctx context.Context, pipe redis.Pipeliner, cmds *[]*redis.Cmd, node *FlowJob, parent *ParentOptions, ids map[*FlowJob]string) (*JobNode, error) {
	prefix := node.Prefix
	if prefix == "" {
		prefix = fp.prefix
	}
	sc := newScripts(fp.conn, prefix, node.QueueName)
	tel := fp.telFor(node.QueueName)

	opts := JobOptions{}
	if node.Opts != nil {
		opts = *node.Opts
	}
	if parent != nil {
		opts.Parent = parent
	}
	if opts.JobID == "" {
		opts.JobID = ids[node] // assigned once by Add, so a retry re-adds the same job
	}

	// One span per node, opened inside the parent node's span (and inside addFlow),
	// so the trace mirrors the tree and each job propagates its own span through tm.
	var out *JobNode
	err := tel.trace(ctx, SpanKindProducer, "addNode", func(ctx context.Context, span Span) error {
		q := &Queue{name: node.QueueName, prefix: prefix, conn: fp.conn,
			keys: NewQueueKeys(node.QueueName, prefix), scripts: sc, tel: tel}
		job, err := newJob(q, node.Name, node.Data, &opts)
		if err != nil {
			return err
		}
		tel.injectTM(ctx, job.opts)
		if span != nil {
			span.SetAttributes(map[string]any{"bullmq.queue": node.QueueName, "bullmq.job.name": node.Name, "bullmq.job.id": job.ID})
		}

		if len(node.Children) > 0 {
			cmd, err := sc.enqueueAddParentJob(ctx, pipe, job)
			if err != nil {
				return err
			}
			*cmds = append(*cmds, cmd)

			childParent := &ParentOptions{ID: job.ID, Queue: sc.keys.QualifiedName()}
			children := make([]*JobNode, 0, len(node.Children))
			for _, child := range node.Children {
				cn, err := fp.addNode(ctx, pipe, cmds, child, childParent, ids)
				if err != nil {
					return err
				}
				children = append(children, cn)
			}
			out = &JobNode{Job: job, Children: children}
			return nil
		}

		cmd, err := sc.enqueueAddJob(ctx, pipe, job)
		if err != nil {
			return err
		}
		*cmds = append(*cmds, cmd)
		out = &JobNode{Job: job}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Close releases resources, closing only clients the producer owns.
func (fp *FlowProducer) Close() error { return fp.conn.Close() }
