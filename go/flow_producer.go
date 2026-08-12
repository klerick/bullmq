package bullmq

import (
	"context"
	"sync"

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
	conn     *connection
	prefix   string
	tel      Telemetry
	loadOnce sync.Once
	loadErr  error
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

// ensureLoaded pre-loads every script (SCRIPT LOAD) once, so EVALSHA succeeds
// inside the MULTI pipeline where the NOSCRIPT fallback is unavailable.
func (fp *FlowProducer) ensureLoaded(ctx context.Context) error {
	fp.loadOnce.Do(func() { fp.loadErr = fp.conn.LoadScripts(ctx) })
	return fp.loadErr
}

// Add inserts a whole flow tree in a single transaction and returns the job tree.
func (fp *FlowProducer) Add(ctx context.Context, flow *FlowJob) (*JobNode, error) {
	if err := fp.ensureLoaded(ctx); err != nil {
		return nil, err
	}

	var tree *JobNode
	err := fp.telFor(flow.QueueName).trace(ctx, SpanKindProducer, "addFlow", func(ctx context.Context, span Span) error {
		if span != nil {
			span.SetAttributes(map[string]any{"bullmq.queue": flow.QueueName, "bullmq.flow.name": flow.Name})
		}
		var cmds []*redis.Cmd
		_, err := fp.conn.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			var e error
			tree, e = fp.addNode(ctx, pipe, &cmds, flow, nil)
			return e
		})
		if err != nil {
			return err
		}
		// Surface any negative status code the scripts returned inside the pipeline.
		for _, cmd := range cmds {
			if res, err := cmd.Result(); err != nil {
				return err
			} else if code, ok := res.(int64); ok && code < 0 {
				return finishedError(ScriptErrorCode(code), errorContext{command: "addJob"})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tree, nil
}

// addNode recursively adds a node and its children, threading parentOpts downward.
// Flow jobs get a generated id up-front so children can reference the parent before
// the pipeline executes.
func (fp *FlowProducer) addNode(ctx context.Context, pipe redis.Pipeliner, cmds *[]*redis.Cmd, node *FlowJob, parent *ParentOptions) (*JobNode, error) {
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
		opts.JobID = genID()
	}

	// One span per node, opened inside the parent node's span (and inside addFlow),
	// so the trace mirrors the tree and each job propagates its own span through tm.
	var out *JobNode
	err := tel.trace(ctx, SpanKindProducer, "addNode", func(ctx context.Context, span Span) error {
		q := &Queue{name: node.QueueName, prefix: prefix, conn: fp.conn,
			keys: NewQueueKeys(node.QueueName, prefix), scripts: sc, tel: tel}
		job := newJob(q, node.Name, node.Data, &opts)
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
				cn, err := fp.addNode(ctx, pipe, cmds, child, childParent)
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
