import { createHash } from 'node:crypto';
import { readFile, readdir } from 'node:fs/promises';
import { join } from 'node:path';
import type { Pool } from 'pg';

const migrationLock = 827497052; // Shared by every ReliQ migration runner.

type AppliedMigration = { name: string; checksum: string };

export class MigrationError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = 'MigrationError';
  }
}

export async function migrate(
  pool: Pool,
  directory: string,
): Promise<string[]> {
  const names = (await readdir(directory))
    .filter((name) => /^\d{3}_[a-z0-9_]+\.sql$/.test(name))
    .sort();
  if (names.length === 0)
    throw new MigrationError('No SQL migration files found');

  const migrations = await Promise.all(
    names.map(async (name) => {
      // Normalize line endings so a Windows checkout has the same checksum.
      const sql = (await readFile(join(directory, name), 'utf8')).replace(
        /\r\n/g,
        '\n',
      );
      const checksum = createHash('sha256').update(sql).digest('hex');
      return { name, sql, checksum };
    }),
  );

  const client = await pool.connect();
  let discardClient = false;
  try {
    // Read current migration history after acquiring the lock.
    await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
    await client.query("SET LOCAL lock_timeout = '10s'");
    await client.query('SELECT pg_advisory_xact_lock($1)', [migrationLock]);
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        name text PRIMARY KEY,
        checksum text NOT NULL,
        applied_at timestamptz NOT NULL DEFAULT now()
      )
    `);

    const { rows: applied } = await client.query<AppliedMigration>(
      'SELECT name, checksum FROM schema_migrations ORDER BY name',
    );
    for (const [index, row] of applied.entries()) {
      const migration = migrations[index];
      if (migration?.name !== row.name || migration.checksum !== row.checksum) {
        throw new MigrationError(`Migration history differs at ${row.name}`);
      }
    }

    const pending = migrations.slice(applied.length);
    for (const { name, sql, checksum } of pending) {
      try {
        await client.query(sql);
        await client.query(
          'INSERT INTO schema_migrations (name, checksum) VALUES ($1, $2)',
          [name, checksum],
        );
      } catch (cause) {
        throw new MigrationError(`Failed to apply ${name}`, { cause });
      }
    }

    await client.query('COMMIT');
    return pending.map(({ name }) => name);
  } catch (error) {
    try {
      await client.query('ROLLBACK');
    } catch {
      discardClient = true;
    }
    throw error;
  } finally {
    client.release(discardClient);
  }
}
