import { randomUUID } from 'node:crypto';
import { createLogger } from '../src/config/logger.js';
import { loadWorkerConfig } from '../src/config/worker.js';
import { DeadLetterArchive } from '../src/db/archive.js';
import { Ledger } from '../src/db/ledger.js';
import { createPostgresPool } from '../src/db/pool.js';
import { sleep } from '../src/jobs/sleep.js';
import { createTransferHandler } from '../src/jobs/transfer.js';
import { Queue } from '../src/queue/queue.js';
import { createRedis } from '../src/queue/redis.js';
import { Worker } from '../src/worker/worker.js';

let logger = createLogger('info');

try {
  const config = loadWorkerConfig();
  const workerId = config.WORKER_ID ?? randomUUID();
  logger = createLogger(config.LOG_LEVEL).child({
    workerId,
    queue: config.QUEUE_NAME,
  });
  const redis = createRedis(config.REDIS_URL, logger);
  const pool = createPostgresPool(config.DATABASE_URL, logger, {
    queryTimeoutMs: 5_000,
  });
  const ledger = new Ledger(pool, logger);
  const archive = new DeadLetterArchive(redis, pool, config.QUEUE_NAME, logger);
  const worker = new Worker(
    new Queue(redis, config.QUEUE_NAME, {
      retryPolicies: config.JOB_RETRY_POLICIES,
      record: (event) => ledger.record(event),
    }),
    new Map([
      ['sleep', sleep],
      ['transfer', createTransferHandler(pool)],
    ]),
    {
      workerId,
      visibilityTimeoutMs: config.VISIBILITY_TIMEOUT_MS,
      heartbeatIntervalMs: config.HEARTBEAT_INTERVAL_MS,
      pollIntervalMs: config.POLL_INTERVAL_MS,
      maintenanceIntervalMs: config.MAINTENANCE_INTERVAL_MS,
      drainTimeoutMs: config.DRAIN_TIMEOUT_MS,
    },
    logger,
  );

  let shutdown: ((signal: 'SIGTERM' | 'SIGINT') => void) | undefined;
  const stopWriters = new AbortController();
  let writers: Promise<void> | undefined;
  try {
    await redis.connect();
    const signalled = new Promise<'SIGTERM' | 'SIGINT'>((resolve) => {
      shutdown = resolve;
    });
    const onTerm = () => shutdown?.('SIGTERM');
    const onInterrupt = () => shutdown?.('SIGINT');
    process.on('SIGTERM', onTerm);
    process.on('SIGINT', onInterrupt);
    try {
      logger.info('Worker started');
      writers = Promise.all([
        ledger.run(stopWriters.signal),
        archive.run(stopWriters.signal),
      ]).then(() => undefined);
      const signal = await Promise.race([
        signalled,
        worker.run().then(() => undefined),
        writers.then(() => undefined),
      ]);
      if (signal) {
        logger.info({ signal }, 'Worker draining');
        if (await worker.drain()) logger.info('Worker drained');
        else {
          logger.warn('Worker drain timed out; outstanding leases will expire');
          // The signal handler removed Node's default exit. An uncooperative handler can keep handles open.
          process.exitCode = 1;
          redis.disconnect();
          process.exit(1);
        }
      }
    } finally {
      process.off('SIGTERM', onTerm);
      process.off('SIGINT', onInterrupt);
    }
  } finally {
    stopWriters.abort();
    redis.disconnect();
    try {
      await writers;
    } finally {
      await pool.end();
    }
  }
} catch (err) {
  logger.error({ err }, 'Worker failed');
  process.exitCode = 1;
}
