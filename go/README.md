# BullMQ for Go

A Go port of [BullMQ](https://github.com/taskforcesh/bullmq), wire-compatible with
the Node.js, Python, Rust and PHP implementations. It shares the same Redis key
schema and the same include-resolved Lua scripts, so a Go producer or consumer can
interoperate with queues driven by any other BullMQ port.

Interoperability is verified by cross-runtime tests against Node BullMQ.

## Install

```sh
go get github.com/klerick/bullmq/go@latest
```

The module currently lives in the `go/` subdirectory of a fork, released under
`go/vX.Y.Z` tags. When the port is merged upstream the import path becomes
`github.com/taskforcesh/bullmq/go` (a one-time change).

Requires Go 1.24+ and Redis 6.2+.

## Quick start

```go
package main

import (
	"context"
	"fmt"

	bullmq "github.com/klerick/bullmq/go"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

	// Produce.
	q, _ := bullmq.NewQueue("emails", bullmq.WithClient(client))
	defer q.Close()
	q.Add(ctx, "welcome", map[string]any{"to": "a@b.c"}, &bullmq.JobOptions{Attempts: 3})

	// Consume.
	w, _ := bullmq.NewWorker("emails", func(ctx context.Context, job *bullmq.Job) (any, error) {
		fmt.Printf("processing %s: %v\n", job.Name, job.Data)
		return "sent", nil
	}, bullmq.WithClient(client))
	defer w.Close()
	w.Run(ctx) // blocks; run in a goroutine in real apps
}
```

## Dependency injection

Constructors accept an already-built `redis.UniversalClient` via `WithClient`, so
the client can come from your DI container. An injected client is never closed by
`Close` (the owner keeps it); a dedicated blocking connection is derived internally
(the equivalent of ioredis `client.duplicate()`). To build the client from options
instead, use `WithRedisOptions`.

## Flows (parent/child)

```go
fp, _ := bullmq.NewFlowProducer(bullmq.WithClient(client))
fp.Add(ctx, &bullmq.FlowJob{
	QueueName: "user", Name: "createUser", Data: userData,
	Children: []*bullmq.FlowJob{{
		QueueName: "tenant", Name: "createTenant", Data: tenantData,
		Children: []*bullmq.FlowJob{
			{QueueName: "project", Name: "createProject", Data: projectData},
			{QueueName: "aud", Name: "createAud", Data: audData},
		},
	}},
})
```

A parent job completes only after all of its children complete; the cascade is
handled Redis-side.

## Schedulers

```go
// Every 5 minutes.
q.UpsertJobScheduler(ctx, "digest", bullmq.RepeatOptions{Every: 5 * 60 * 1000}, "digest", nil, nil)

// Cron (verified against Node's cron-parser).
q.UpsertJobScheduler(ctx, "nightly", bullmq.RepeatOptions{Pattern: "0 3 * * *"}, "cleanup", nil, nil)
```

## Cross-process events

```go
qe, _ := bullmq.NewQueueEvents("emails", bullmq.WithClient(client))
defer qe.Close()
events, errs := qe.Listen(ctx)
for e := range events {
	fmt.Println(e.Event, e.JobID)
}
_ = errs
```

## Status

See [FEATURE_PARITY.md](./FEATURE_PARITY.md). The core is complete (Queue, Worker,
Job, FlowProducer, JobScheduler, QueueEvents); metrics/Prometheus and Redis
Cluster/Sentinel are not yet implemented.

## License

MIT, same as BullMQ.
