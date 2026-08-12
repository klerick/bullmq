// Reads job counts with Node's BullMQ so the Go port's counting can be compared
// against the reference implementation on the very same Redis state.
// Usage: node counts.mjs <prefix> <queueName> <type,type,...>
// Prints {"<type>": <count>, ...} on stdout. Connection comes from REDIS_HOST/PORT/DB.
import { Queue } from 'bullmq';

const [, , prefix, queueName, types] = process.argv;

const connection = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379', 10),
  db: parseInt(process.env.REDIS_DB || '0', 10),
};

const queue = new Queue(queueName, { connection, prefix });
try {
  const counts = await queue.getJobCounts(...types.split(','));
  process.stdout.write(JSON.stringify(counts));
} finally {
  await queue.close();
}
