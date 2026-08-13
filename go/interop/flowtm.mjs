// Adds a two-node flow with Node's BullMQ under a *fake* Telemetry, so the Go
// port can compare what each runtime stores in a flow job's `tm` option.
// Usage: node flowtm.mjs <prefix> <parentQueue> <childQueue>
// Prints {"parent":"<id>","child":"<id>"} on stdout.
//
// The parent node is given opts, the child node none: that is the difference
// flow-producer.ts:394 (`if (srcPropagationMetadata && opts)`) turns into
// "context propagated" vs "not propagated". No OpenTelemetry SDK is needed —
// BullMQ's Telemetry interface is small enough to implement here, and the test
// only asks whether metadata was written at all.
import { FlowProducer } from 'bullmq';

const [, , prefix, parentQueue, childQueue] = process.argv;

const connection = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379', 10),
  db: parseInt(process.env.REDIS_DB || '0', 10),
};

// Contexts are plain objects holding the active span; metadata is that span's
// name, which makes the stored value readable in test failures.
const makeTelemetry = () => {
  let active = {};
  const tracer = {
    startSpan(name) {
      const span = {
        name,
        setSpanOnContext: ctx => ({ ...ctx, span }),
        setAttribute() {},
        setAttributes() {},
        addEvent() {},
        recordException() {},
        end() {},
      };
      return span;
    },
  };
  const contextManager = {
    active: () => active,
    with(ctx, fn) {
      const prev = active;
      active = ctx;
      try {
        return fn();
      } finally {
        active = prev;
      }
    },
    getMetadata: ctx => (ctx && ctx.span ? `span:${ctx.span.name}` : ''),
    fromMetadata: (activeCtx, metadata) => ({
      ...activeCtx,
      restored: metadata,
    }),
  };
  return { tracer, contextManager };
};

const flow = new FlowProducer({
  connection,
  prefix,
  telemetry: makeTelemetry(),
});

try {
  const tree = await flow.add({
    name: 'parent',
    queueName: parentQueue,
    opts: { attempts: 1 }, // node WITH opts
    children: [
      {
        name: 'child',
        queueName: childQueue, // node WITHOUT opts
      },
    ],
  });
  process.stdout.write(
    JSON.stringify({ parent: tree.job.id, child: tree.children[0].job.id }),
  );
} finally {
  await flow.close();
}
