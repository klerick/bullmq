// Adds a job with Node's BullMQ so a Go worker can consume it.
// Usage: node add.mjs <prefix> <queueName> <jobName> <dataJSON>
// Prints the created job id on stdout. Connection comes from REDIS_HOST/PORT/DB.
import { Queue } from 'bullmq';

const [, , prefix, queueName, jobName, dataJSON] = process.argv;

const connection = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379', 10),
  db: parseInt(process.env.REDIS_DB || '0', 10),
};

const queue = new Queue(queueName, { connection, prefix });
try {
  const job = await queue.add(jobName, JSON.parse(dataJSON || '{}'));
  process.stdout.write(job.id);
} finally {
  await queue.close();
}
