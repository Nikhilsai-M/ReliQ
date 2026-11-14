import { Pool } from 'pg';
import type { Logger } from 'pino';

export function createPostgresPool(
  connectionString: string,
  logger: Logger,
): Pool {
  const pool = new Pool({ connectionString, connectionTimeoutMillis: 3_000 });
  pool.on('error', (err) =>
    logger.warn({ err }, 'Idle PostgreSQL connection error'),
  );
  return pool;
}
