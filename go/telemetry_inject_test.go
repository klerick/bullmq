package bullmq

import (
	"context"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

type spanPathKey struct{}

// pathTelemetry records the span stack in the context, so a test can assert *which*
// span's context ended up injected into a job. Inject returns the ">"-joined names
// of the spans wrapping the call, mirroring how a real tracer serialises the active
// span: the metadata a job carries must come from the span that produced it.
type pathTelemetry struct {
	mu        sync.Mutex
	spans     []mockSpanRec
	extracted []string
}

func (p *pathTelemetry) StartSpan(ctx context.Context, name string, kind SpanKind) (context.Context, Span) {
	p.mu.Lock()
	p.spans = append(p.spans, mockSpanRec{name, kind})
	p.mu.Unlock()
	path := name
	if prev, _ := ctx.Value(spanPathKey{}).(string); prev != "" {
		path = prev + ">" + name
	}
	return context.WithValue(ctx, spanPathKey{}, path), mockSpan{}
}

func (p *pathTelemetry) Inject(ctx context.Context) string {
	s, _ := ctx.Value(spanPathKey{}).(string)
	return s
}

func (p *pathTelemetry) Extract(ctx context.Context, metadata string) context.Context {
	p.mu.Lock()
	p.extracted = append(p.extracted, metadata)
	p.mu.Unlock()
	return context.WithValue(ctx, spanPathKey{}, metadata)
}

func (p *pathTelemetry) sawExtracted(metadata string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, m := range p.extracted {
		if m == metadata {
			return true
		}
	}
	return false
}

func (p *pathTelemetry) has(name string, kind SpanKind) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, s := range p.spans {
		if s.name == name && s.kind == kind {
			return true
		}
	}
	return false
}

// offlineClient builds a client that is never dialled: the flow tests below only
// queue commands on a pipeline, which needs no server.
func offlineClient(t *testing.T) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// A FlowProducer must keep the configured telemetry, like Queue does — otherwise
// WithTelemetry is silently dropped in the constructor.
func TestFlowProducerKeepsTelemetry(t *testing.T) {
	tel := &pathTelemetry{}
	fp, err := NewFlowProducer(WithClient(offlineClient(t)), WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}
	if fp.tel != Telemetry(tel) {
		t.Fatalf("telemetry lost in constructor: got %v", fp.tel)
	}
}

// Every node of a flow tree carries trace context, and it is the node's own span
// that gets injected — so the consumer of a child continues the trace under that
// child's producer span, exactly as in the TS port (flow-producer.ts addNode).
func TestFlowProducerAddNodeInjectsTM(t *testing.T) {
	tel := &pathTelemetry{}
	client := offlineClient(t)
	fp, err := NewFlowProducer(WithClient(client), WithTelemetry(tel))
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}

	flow := &FlowJob{
		Name: "parent", QueueName: "flow-parent",
		Children: []*FlowJob{{Name: "child", QueueName: "flow-child"}},
	}
	var cmds []*redis.Cmd
	tree, err := fp.addNode(context.Background(), client.Pipeline(), &cmds, flow, nil)
	if err != nil {
		t.Fatalf("addNode: %v", err)
	}

	if got := toStr(tree.Job.opts["tm"]); got != "flow-parent.addNode" {
		t.Errorf("parent tm = %q, want %q", got, "flow-parent.addNode")
	}
	// The child's span nests inside the parent's, so its metadata carries both.
	child := tree.Children[0]
	if got := toStr(child.Job.opts["tm"]); got != "flow-parent.addNode>flow-child.addNode" {
		t.Errorf("child tm = %q, want the parent-nested path", got)
	}
	// Span names are per node queue, not per producer (a flow spans many queues).
	if !tel.has("flow-parent.addNode", SpanKindProducer) || !tel.has("flow-child.addNode", SpanKindProducer) {
		t.Errorf("missing per-node producer spans, got %v", tel.spans)
	}
}

// Trace metadata set explicitly on the job wins over the ambient span, matching
// upstream's `opts.telemetry?.metadata || srcPropagationMetadata` precedence.
func TestInjectTMKeepsExplicitMetadata(t *testing.T) {
	tel := &pathTelemetry{}
	h := telemetryHelper{t: tel, name: "q"}
	ctx, span := h.start(context.Background(), SpanKindProducer, "add")
	defer span.finish(nil)

	job := mustNewJob(t, nil, "task", nil, &JobOptions{Extra: map[string]any{"tm": "explicit"}})
	h.injectTM(ctx, job.opts)
	if got := toStr(job.opts["tm"]); got != "explicit" {
		t.Errorf("tm = %q, want the explicitly set %q", got, "explicit")
	}

	plain := mustNewJob(t, nil, "task", nil, nil)
	h.injectTM(ctx, plain.opts)
	if got := toStr(plain.opts["tm"]); got != "q.add" {
		t.Errorf("tm = %q, want the active span %q", got, "q.add")
	}
}
