import { Pool } from 'pg';
import type { Logger } from 'pino';

export function createPostgresPool(
  connectionString: string,
  logger: Logger,
  options?: { queryTimeoutMs: number },
): Pool {
  const timeouts = options
    ? {
        statement_timeout: options.queryTimeoutMs,
        query_timeout: options.queryTimeoutMs + 1_000,
      }
    : {};
  const pool = new Pool({
    connectionString,
    connectionTimeoutMillis: 3_000,
    ...timeouts,
  });
  pool.on('error', (err) =>
    logger.warn({ err }, 'Idle PostgreSQL connection error'),
  );
  return pool;
}
