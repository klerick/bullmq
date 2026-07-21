// Reads a job with Node's BullMQ so a Go-produced job can be verified.
// Usage: node read.mjs <prefix> <queueName> <jobId>
// Prints the job as JSON on stdout. Connection comes from REDIS_HOST/PORT/DB.
import { Queue } from 'bullmq';

const [, , prefix, queueName, jobId] = process.argv;

const connection = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379', 10),
  db: parseInt(process.env.REDIS_DB || '0', 10),
};

const queue = new Queue(queueName, { connection, prefix });
try {
  const job = await queue.getJob(jobId);
  if (!job) {
    console.error(`job ${jobId} not found`);
    process.exit(2);
  }
  process.stdout.write(
    JSON.stringify({
      id: job.id,
      name: job.name,
      data: job.data,
      opts: job.opts,
      returnvalue: job.returnvalue,
      failedReason: job.failedReason,
    }),
  );
} finally {
  await queue.close();
}
