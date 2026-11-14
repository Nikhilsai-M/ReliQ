import { randomUUID } from 'node:crypto';
import { copyFile, mkdtemp, rm, rmdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import type { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { loadConfig } from '../../src/config/env.js';
import { createLogger } from '../../src/config/logger.js';
import { MigrationError, migrate } from '../../src/db/migrate.js';
import { createPostgresPool } from '../../src/db/pool.js';
import { createRedis } from '../../src/queue/redis.js';

const { REDIS_URL, DATABASE_URL, LOG_LEVEL } = loadConfig();
const logger = createLogger(LOG_LEVEL);
const migrationsDirectory = resolve('src/db/migrations');

it('connects to Redis with AOF enabled and one-second fsync', async () => {
  const redis = createRedis(REDIS_URL, logger);
  try {
    await redis.connect();
    expect(await redis.ping()).toBe('PONG');
    expect(await redis.config('GET', 'appendonly')).toEqual([
      'appendonly',
      'yes',
    ]);
    expect(await redis.config('GET', 'appendfsync')).toEqual([
      'appendfsync',
      'everysec',
    ]);
  } finally {
    redis.disconnect();
  }
});

describe('PostgreSQL migrations', () => {
  const admin = createPostgresPool(DATABASE_URL, logger);
  const schema = `reliq_test_${randomUUID().replaceAll('-', '')}`;
  let pool: Pool | undefined;
  let schemaCreated = false;

  beforeAll(async () => {
    // The name is generated locally from a UUID and contains only letters, digits and underscores.
    await admin.query(`CREATE SCHEMA ${schema}`);
    schemaCreated = true;
    const url = new URL(DATABASE_URL);
    const options = url.searchParams.get('options') ?? '';
    url.searchParams.set(
      'options',
      `${options} -c search_path=${schema}`.trim(),
    );
    pool = createPostgresPool(url.href, logger);
  });

  afterAll(async () => {
    try {
      await pool?.end();
      if (schemaCreated) await admin.query(`DROP SCHEMA ${schema} CASCADE`);
    } finally {
      await admin.end();
    }
  });

  it('serializes concurrent migrations, records each file once and installs constraints', async () => {
    if (!pool) throw new Error('Test schema was not initialized');
    const results = await Promise.all([
      migrate(pool, migrationsDirectory),
      migrate(pool, migrationsDirectory),
    ]);
    expect(results).toContainEqual([
      '001_queue_history.sql',
      '002_idempotency_demo.sql',
      '003_preserve_archive_json.sql',
      '004_transfer_observations.sql',
    ]);
    expect(results).toContainEqual([]);
    expect(await migrate(pool, migrationsDirectory)).toEqual([]);

    const tables = await pool.query<{ table_name: string }>(
      'SELECT table_name FROM information_schema.tables WHERE table_schema = $1 ORDER BY table_name',
      [schema],
    );
    expect(tables.rows.map(({ table_name }) => table_name)).toEqual([
      'accounts',
      'dead_letter',
      'idempotency_keys',
      'jobs_ledger',
      'receipts',
      'schema_migrations',
      'transfer_effects',
      'transfer_executions',
      'transfers',
    ]);

    const jobId = randomUUID();
    const insert =
      'INSERT INTO idempotency_keys (job_id, step_key) VALUES ($1, $2)';
    await pool.query(insert, [jobId, 'debit']);
    await pool.query(insert, [jobId, 'credit']);
    await expect(pool.query(insert, [jobId, 'debit'])).rejects.toMatchObject({
      code: '23505',
    });
  });

  it('rolls back schema changes and migration history when a pending file fails', async () => {
    if (!pool) throw new Error('Test schema was not initialized');
    await migrate(pool, migrationsDirectory);
    const directory = await mkdtemp(join(tmpdir(), 'reliq-migrations-'));
    const history = [
      '001_queue_history.sql',
      '002_idempotency_demo.sql',
      '003_preserve_archive_json.sql',
      '004_transfer_observations.sql',
    ];
    const probe = '005_probe.sql';
    const fails = '006_fails.sql';
    try {
      for (const name of history) {
        await copyFile(join(migrationsDirectory, name), join(directory, name));
      }
      await writeFile(
        join(directory, probe),
        'CREATE TABLE rollback_probe (id int);',
      );
      await writeFile(join(directory, fails), 'SELECT 1 / 0;');

      await expect(migrate(pool, directory)).rejects.toThrow(MigrationError);
      const result = await pool.query<{ probe: string | null }>(
        "SELECT to_regclass('rollback_probe') AS probe",
      );
      expect(result.rows[0]?.probe).toBeNull();
      expect(await migrate(pool, migrationsDirectory)).toEqual([]);
    } finally {
      for (const name of [...history, probe, fails]) {
        await rm(join(directory, name), { force: true });
      }
      await rmdir(directory);
    }
  });
});
