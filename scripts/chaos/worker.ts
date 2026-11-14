import { setTimeout } from 'node:timers/promises';
import { loadConfig } from '../../src/config/env.js';
import { createLogger } from '../../src/config/logger.js';
import { createPostgresPool } from '../../src/db/pool.js';
import { createTransferHandler } from '../../src/jobs/transfer.js';
import { Queue } from '../../src/queue/queue.js';
import { createRedis } from '../../src/queue/redis.js';
import { Worker } from '../../src/worker/worker.js';

const config = loadConfig();
const [name, workerId, pauseId] = process.argv.slice(2);
if (!name || !workerId || !process.send)
  throw new Error('Start this worker through the chaos harness');
const logger = createLogger(config.LOG_LEVEL);
const redis = createRedis(config.REDIS_URL, logger);
const pool = createPostgresPool(config.DATABASE_URL, logger, {
  queryTimeoutMs: 5_000,
});
const handler = createTransferHandler(pool, async (job, signal) => {
  if (job.jobId !== pauseId) return;
  process.send?.({ event: 'debit_committed', jobId: job.jobId });
  // The parent kills this process at the transaction boundary.
  await setTimeout(300_000, undefined, { signal });
});
const queue = new Queue(redis, name, {
  record(event) {
    if (event.to === 'succeeded')
      process.send?.({ event: 'completed', jobId: event.jobId });
  },
});
const worker = new Worker(
  queue,
  new Map([['transfer', handler]]),
  {
    workerId,
    visibilityTimeoutMs: 2_000,
    heartbeatIntervalMs: 250,
    pollIntervalMs: 20,
    maintenanceIntervalMs: 50,
    drainTimeoutMs: 5_000,
  },
  logger,
);
const stop = () => {
  void worker.drain();
};
process.once('SIGTERM', stop);
process.once('disconnect', stop);
try {
  await redis.connect();
  process.send({ event: 'ready' });
  await worker.run();
} finally {
  redis.disconnect();
  await pool.end();
  process.disconnect?.();
}
