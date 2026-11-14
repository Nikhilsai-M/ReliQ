import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import type { Pool } from 'pg';
import { pino } from 'pino';
import { afterAll, beforeAll, expect, it } from 'vitest';
import { loadConfig } from '../../src/config/env.js';
import { createLogger } from '../../src/config/logger.js';
import { DeadLetterArchive } from '../../src/db/archive.js';
import { Ledger } from '../../src/db/ledger.js';
import { migrate } from '../../src/db/migrate.js';
import { createPostgresPool } from '../../src/db/pool.js';
import { queueKeys } from '../../src/queue/keys.js';
import { Queue } from '../../src/queue/queue.js';
import { createRedis } from '../../src/queue/redis.js';

const config = loadConfig();
const logger = createLogger('silent');
const redis = createRedis(config.REDIS_URL, logger);
const admin = createPostgresPool(config.DATABASE_URL, logger);
const name = `history-${randomUUID()}`;
const keys = queueKeys(name);
const schema = `reliq_test_${randomUUID().replaceAll('-', '')}`;
const migrations = resolve('src/db/migrations');
const ids: string[] = [];
let pool: Pool;
let schemaCreated = false;

beforeAll(async () => {
  await redis.connect();
  await admin.query(`CREATE SCHEMA ${schema}`);
  schemaCreated = true;
  const url = new URL(config.DATABASE_URL);
  const options = url.searchParams.get('options') ?? '';
  url.searchParams.set('options', `${options} -c search_path=${schema}`.trim());
  pool = createPostgresPool(url.href, logger);
});

afterAll(async () => {
  try {
    if (redis.status === 'ready') {
      await redis.unlink(
        keys.ready,
        keys.delayed,
        keys.inFlight,
        keys.dead,
        keys.dlqArchive,
        ...ids.map(keys.job),
      );
    }
  } finally {
    redis.disconnect();
    await pool?.end();
    try {
      if (schemaCreated) await admin.query(`DROP SCHEMA ${schema} CASCADE`);
    } finally {
      await admin.end();
    }
  }
});

it('preserves live recovery while PostgreSQL writes fail, then drains the ledger and archive', async () => {
  let ledgerWriteFailed = false;
  const ledgerLogger = pino(
    { level: 'warn' },
    {
      write(line) {
        if (line.includes('Ledger write failed; retry scheduled'))
          ledgerWriteFailed = true;
      },
    },
  );
  const ledger = new Ledger(pool, ledgerLogger, 4);
  const stop = new AbortController();
  const writing = ledger.run(stop.signal);
  const archive = new DeadLetterArchive(redis, pool, name, logger);
  const queue = new Queue(redis, name, {
    record: (event) => ledger.record(event),
  });
  // PostgreSQL cannot write until the isolated schema has been migrated.
  const payload = { data: 'nul\0and lone surrogate\uD800' };
  const error = new Error('broken handler');
  try {
    const { id } = await queue.enqueue({
      type: 'demo',
      payload,
      maxAttempts: 1,
    });
    ids.push(id);
    const claim = await queue.claim('worker', 30_000);
    if (!claim) throw new Error('Expected claim');
    await queue.fail(claim, error);
    await expect(archive.flush()).rejects.toMatchObject({ code: '42P01' });
    expect(await redis.xlen(keys.dlqArchive)).toBe(1);
    expect(await queue.listDead()).toMatchObject([
      { id, payload, attempts: 1, stack: error.stack },
    ]);

    await queue.replay({ id, replayCount: 0 });
    const second = await queue.claim('worker', 30_000);
    if (!second) throw new Error('Expected replay claim');
    await queue.fail(second, 'second failure');
    expect(await redis.xlen(keys.dlqArchive)).toBe(3);
    await expect.poll(() => ledgerWriteFailed).toBe(true);

    await migrate(pool, migrations);
    // Two archive writers may read the same entries. Committed events must remain idempotent.
    await Promise.all([archive.flush(), archive.flush()]);
    expect(await redis.xlen(keys.dlqArchive)).toBe(0);
    const { rows } = await pool.query(
      `SELECT replay_count, replayed_at, last_error, stack, attempts, payload
      FROM dead_letter WHERE job_id = $1 ORDER BY replay_count`,
      [id],
    );
    expect(rows).toEqual([
      {
        replay_count: 0,
        replayed_at: expect.any(Date),
        last_error: 'broken handler',
        stack: error.stack,
        attempts: 1,
        payload,
      },
      {
        replay_count: 1,
        replayed_at: null,
        last_error: 'second failure',
        stack: null,
        attempts: 1,
        payload,
      },
    ]);
    expect(await queue.listDead()).toMatchObject([{ id, replayCount: 1 }]);

    const history = async () =>
      (
        await pool.query(
          `SELECT "from", "to", worker_id, attempt, detail
      FROM jobs_ledger WHERE job_id = $1 ORDER BY id`,
          [id],
        )
      ).rows;
    await expect.poll(history).toHaveLength(4);
    expect(await history()).toMatchObject([
      { from: null, to: 'ready', attempt: 0, detail: { payload } },
      { from: 'ready', to: 'in_flight', attempt: 1, worker_id: 'worker' },
      {
        from: 'in_flight',
        to: 'dead',
        attempt: 1,
        detail: { lastError: 'broken handler' },
      },
      { from: 'dead', to: 'ready', attempt: 0, detail: { replayCount: 1 } },
    ]);
  } finally {
    stop.abort();
    await writing;
  }
});
