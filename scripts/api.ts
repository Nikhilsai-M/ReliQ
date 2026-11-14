import { createServer } from 'node:http';
import { createApp } from '../src/api/app.js';
import { createHealthCheck } from '../src/api/health.js';
import { loadApiConfig } from '../src/config/api.js';
import { createLogger } from '../src/config/logger.js';
import { Ledger } from '../src/db/ledger.js';
import { createPostgresPool } from '../src/db/pool.js';
import { Queue } from '../src/queue/queue.js';
import { createRedis } from '../src/queue/redis.js';

let logger = createLogger('info');
try {
  const config = loadApiConfig();
  logger = createLogger(config.LOG_LEVEL);
  const redis = createRedis(config.REDIS_URL, logger);
  const pool = createPostgresPool(config.DATABASE_URL, logger, {
    queryTimeoutMs: 5_000,
  });
  const ledger = new Ledger(pool, logger);
  const queues = new Map(
    config.API_QUEUES.map((name) => [
      name,
      new Queue(redis, name, {
        retryPolicies: config.JOB_RETRY_POLICIES,
        record: (event) => ledger.record(event),
      }),
    ]),
  );
  const app = createApp(
    (name) => queues.get(name),
    logger,
    createHealthCheck(redis, pool),
  );
  const server = createServer(app);
  const stop = new AbortController();
  const shutdown = () => server.close();
  let writing: Promise<void> | undefined;
  try {
    await redis.connect();
    const closed = new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.once('close', resolve);
    });
    process.once('SIGTERM', shutdown);
    process.once('SIGINT', shutdown);
    writing = ledger.run(stop.signal);
    server.listen(config.API_PORT, config.API_HOST, () => {
      logger.info({ port: config.API_PORT }, 'Queue API listening');
    });
    await Promise.race([closed, writing]);
  } finally {
    process.off('SIGTERM', shutdown);
    process.off('SIGINT', shutdown);
    if (server.listening) server.close();
    stop.abort();
    redis.disconnect();
    try {
      await writing;
    } finally {
      await pool.end();
    }
  }
} catch (err) {
  logger.error({ err }, 'Queue API failed');
  process.exitCode = 1;
}
