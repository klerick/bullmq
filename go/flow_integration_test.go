package bullmq

import (
	"context"
	"sync"
	"testing"
	"time"
)

// The day-one target scenario: user -> tenant -> (project, aud). A parent must
// complete only after all its children, driven by the Redis-side cascade in
// moveToFinished. No explicit moveToWaitingChildren is needed for a static tree.
func TestFlowProducerCascade(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	fp, err := NewFlowProducer(WithClient(client))
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}
	defer fp.Close()

	tree, err := fp.Add(ctx, &FlowJob{
		QueueName: "user", Name: "createUser", Data: map[string]any{"email": "a@b.c"},
		Children: []*FlowJob{{
			QueueName: "tenant", Name: "createTenant", Data: map[string]any{"name": "acme"},
			Children: []*FlowJob{
				{QueueName: "project", Name: "createProject", Data: map[string]any{"demo": true}},
				{QueueName: "aud", Name: "createAud", Data: map[string]any{"scope": "authz"}},
			},
		}},
	})
	if err != nil {
		t.Fatalf("flow Add: %v", err)
	}
	if tree.Job.ID == "" || len(tree.Children) != 1 || len(tree.Children[0].Children) != 2 {
		t.Fatalf("unexpected tree shape: %+v", tree)
	}

	var mu sync.Mutex
	var order []string
	var childValues map[string]any
	userDone := make(chan struct{}, 1)

	record := func(name string) {
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
	}
	start := func(queue string, proc Processor) *Worker {
		w, err := NewWorker(queue, proc, WithClient(client))
		if err != nil {
			t.Fatalf("NewWorker(%s): %v", queue, err)
		}
		go func() { _ = w.Run(ctx) }()
		return w
	}

	leaf := func(ctx context.Context, j *Job) (any, error) { record(j.Name); return "v:" + j.Name, nil }
	wProject := start("project", leaf)
	wAud := start("aud", leaf)
	wTenant := start("tenant", leaf)
	wUser := start("user", func(ctx context.Context, j *Job) (any, error) {
		cv, _ := j.GetChildrenValues(ctx)
		mu.Lock()
		childValues = cv
		mu.Unlock()
		record(j.Name)
		userDone <- struct{}{}
		return "v:createUser", nil
	})
	defer wProject.Close()
	defer wAud.Close()
	defer wTenant.Close()
	defer wUser.Close()

	select {
	case <-userDone:
	case <-time.After(20 * time.Second):
		mu.Lock()
		o := append([]string(nil), order...)
		mu.Unlock()
		t.Fatalf("flow did not complete in time; completed so far = %v", o)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 4 {
		t.Fatalf("expected 4 completions, got %v", order)
	}
	pos := map[string]int{}
	for i, n := range order {
		pos[n] = i
	}
	if pos["createUser"] != 3 {
		t.Errorf("parent (user) must complete last; order = %v", order)
	}
	if pos["createTenant"] >= pos["createUser"] {
		t.Errorf("tenant must complete before user; order = %v", order)
	}
	if pos["createProject"] >= pos["createTenant"] || pos["createAud"] >= pos["createTenant"] {
		t.Errorf("children must complete before tenant; order = %v", order)
	}
	// The parent can read its children's return values from the processed hash.
	if len(childValues) == 0 {
		t.Error("GetChildrenValues empty; cascade did not record child results")
	}
}

// Dependency inspection and manual unlinking: a freshly added tenant has two
// pending children; removing one child dependency drops the count to one.
func TestFlowDependencyOps(t *testing.T) {
	client := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	fp, err := NewFlowProducer(WithClient(client))
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}
	defer fp.Close()

	tree, err := fp.Add(ctx, &FlowJob{
		QueueName: "tenant", Name: "createTenant", Data: nil,
		Children: []*FlowJob{
			{QueueName: "project", Name: "createProject", Data: nil},
			{QueueName: "aud", Name: "createAud", Data: nil},
		},
	})
	if err != nil {
		t.Fatalf("flow Add: %v", err)
	}
	tenant := tree.Job
	project := tree.Children[0].Job

	if n, err := tenant.GetDependenciesCount(ctx); err != nil || n != 2 {
		t.Fatalf("tenant dependencies = %d (err %v), want 2", n, err)
	}

	if err := project.queue.scripts.removeChildDependency(ctx, project.ID, project.ParentKey); err != nil {
		t.Fatalf("removeChildDependency: %v", err)
	}

	if n, err := tenant.GetDependenciesCount(ctx); err != nil || n != 1 {
		t.Fatalf("tenant dependencies after removal = %d (err %v), want 1", n, err)
	}
}
