import { resolve } from 'node:path';
import { loadConfig } from '../src/config/env.js';
import { createLogger } from '../src/config/logger.js';
import { migrate } from '../src/db/migrate.js';
import { createPostgresPool } from '../src/db/pool.js';

let logger = createLogger('info');

try {
  const config = loadConfig();
  logger = createLogger(config.LOG_LEVEL);
  const pool = createPostgresPool(config.DATABASE_URL, logger);
  try {
    const applied = await migrate(pool, resolve('src/db/migrations'));
    logger.info({ applied }, 'PostgreSQL migrations complete');
  } finally {
    await pool.end();
  }
} catch (err) {
  logger.error({ err }, 'Migration failed');
  process.exitCode = 1;
}
