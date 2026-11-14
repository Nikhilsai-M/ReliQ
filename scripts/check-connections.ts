import { loadConfig } from '../src/config/env.js';
import { createLogger } from '../src/config/logger.js';
import { createPostgresPool } from '../src/db/pool.js';
import { createRedis } from '../src/queue/redis.js';

let logger = createLogger('info');

try {
  const config = loadConfig();
  logger = createLogger(config.LOG_LEVEL);
  const redis = createRedis(config.REDIS_URL, logger);
  const pool = createPostgresPool(config.DATABASE_URL, logger);

  try {
    await redis.connect();
    const pong = await redis.ping();
    if (pong !== 'PONG')
      throw new Error(`Unexpected Redis ping response: ${pong}`);
    logger.info({ reply: pong }, 'Redis connected');

    const result = await pool.query<{ value: number }>('SELECT 1 AS value');
    if (result.rows[0]?.value !== 1)
      throw new Error('Unexpected PostgreSQL probe result');
    logger.info('PostgreSQL connected');
  } finally {
    redis.disconnect();
    await pool.end();
  }
} catch (err) {
  logger.error({ err }, 'Infrastructure check failed');
  process.exitCode = 1;
}
