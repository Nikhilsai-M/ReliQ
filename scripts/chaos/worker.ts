import { setTimeout } from 'node:timers/promises';
import { loadConfig } from '../../src/config/env.js';
import { createLogger } from '../../src/config/logger.js';
import { createPostgresPool } from '../../src/db/pool.js';
import { createTransferHandler } from '../../src/jobs/transfer.js';
import { Queue, type ClaimCredential } from '../../src/queue/queue.js';
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
const transfer = createTransferHandler(
  pool,
  async (job, signal) => {
    if (job.jobId !== pauseId) return;
    process.send?.({ event: 'debit_committed', jobId: job.jobId });
    // The parent kills this process at the transaction boundary.
    await setTimeout(300_000, undefined, { signal });
  },
  (job) => {
    process.send?.({
      event: 'handler_started',
      jobId: job.jobId,
      claimId: job.claimId,
      atMs: Date.now(),
    });
  },
);
class TracedQueue extends Queue {
  override async ack(claim: ClaimCredential) {
    const { jobId, claimId } = claim;
    try {
      await super.ack(claim);
      process.send?.({ event: 'completed', jobId, claimId, atMs: Date.now() });
    } catch (error) {
      process.send?.({ event: 'ack_failed', jobId, claimId, atMs: Date.now() });
      throw error;
    }
  }
}
const queue = new TracedQueue(redis, name);
const worker = new Worker(
  queue,
  new Map([['transfer', transfer]]),
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
