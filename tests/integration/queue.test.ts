import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
} from 'vitest';
import { ZodError } from 'zod';
import { loadConfig } from '../../src/config/env.js';
import { createLogger } from '../../src/config/logger.js';
import {
  InvalidClaimError,
  JobExistsError,
  JobNotFoundError,
  QueueInvariantError,
  ReplayConflictError,
} from '../../src/queue/errors.js';
import { queueKeys } from '../../src/queue/keys.js';
import { readyScore } from '../../src/queue/priority.js';
import { Queue } from '../../src/queue/queue.js';
import { createRedis } from '../../src/queue/redis.js';
import { runScript, scripts } from '../../src/queue/scripts.js';

const config = loadConfig();
const logger = createLogger(config.LOG_LEVEL);
const redis = createRedis(config.REDIS_URL, logger);
const leaseMs = 30_000;
const job = {
  type: 'test',
  payload: { message: 'hello', nested: [null, 1, true] },
};
let queueName: string;
let queue: Queue;
let keys: ReturnType<typeof queueKeys>;

async function redisTime() {
  const [seconds, micros] = await redis.time();
  return Number(seconds) * 1000 + Math.floor(Number(micros) / 1000);
}

async function waitUntil(timestamp: number) {
  await expect
    .poll(() => redisTime(), { timeout: 2_500 })
    .toBeGreaterThanOrEqual(timestamp);
}

async function claimOne() {
  const claimed = await queue.claim('worker-1', leaseMs);
  if (!claimed) throw new Error('Expected a claim');
  return claimed;
}

beforeAll(() => redis.connect());
afterAll(() => redis.disconnect());

beforeEach(() => {
  queueName = `test-${randomUUID()}`;
  queue = new Queue(redis, queueName);
  keys = queueKeys(queueName);
});

afterEach(async () => {
  // Clean up only the isolated namespace allocated to this test.
  let cursor = '0';
  do {
    const [next, matches] = await redis.scan(
      cursor,
      'MATCH',
      `reliq:{${queueName}}:*`,
    );
    if (matches.length > 0) await redis.unlink(...matches);
    cursor = next;
  } while (cursor !== '0');
});

describe('enqueue', () => {
  it('initializes a missing job and writes the exact ready score', async () => {
    const before = await redisTime();
    const enqueued = await queue.enqueue({
      ...job,
      priority: 'low',
      maxAttempts: 5,
    });
    expect(enqueued.status).toBe('ready');
    expect(enqueued.enqueuedAt).toBeGreaterThanOrEqual(before);
    expect(enqueued.enqueuedAt).toBeLessThanOrEqual(await redisTime());
    expect(enqueued.runAt).toBe(enqueued.enqueuedAt);
    expect(await redis.hgetall(keys.job(enqueued.id))).toMatchObject({
      payload: JSON.stringify(job.payload),
      attempts: '0',
      maxAttempts: '5',
      fencingToken: '0',
      status: 'ready',
    });
    expect(await redis.zscore(keys.ready, enqueued.id)).toBe(
      String(readyScore('low', enqueued.enqueuedAt)),
    );
  });

  it('rejects a repeated ID without changing the original job', async () => {
    const { id } = await queue.enqueue(job);
    const saved = await redis.hgetall(keys.job(id));
    await expect(
      queue.enqueue({ ...job, id, priority: 'high', delayMs: 5_000 }),
    ).rejects.toBeInstanceOf(JobExistsError);
    expect(await redis.hgetall(keys.job(id))).toEqual(saved);
    expect(await redis.zcard(keys.ready)).toBe(1);
    expect(await redis.zcard(keys.delayed)).toBe(0);
  });

  it('preserves all JSON payload fields through claim', async () => {
    const payload = {
      ['__proto__']: { label: 'user data' },
      constructor: null,
    };
    await queue.enqueue({ ...job, payload });
    expect((await claimOne()).payload).toStrictEqual(payload);
  });

  it('supports relative and absolute schedules and treats an overdue runAt as ready', async () => {
    const at = (await redisTime()) + 60_000;
    const relative = await queue.enqueue({ ...job, delayMs: 60_000 });
    const absolute = await queue.enqueue({ ...job, runAt: at });
    const overdue = await queue.enqueue({ ...job, runAt: at - 120_000 });
    expect(relative).toMatchObject({
      status: 'delayed',
      runAt: relative.enqueuedAt + 60_000,
    });
    expect(absolute).toMatchObject({ status: 'delayed', runAt: at });
    expect(overdue.status).toBe('ready');
    expect(await redis.zscore(keys.delayed, relative.id)).toBe(
      String(relative.runAt),
    );
    expect(await redis.zscore(keys.delayed, absolute.id)).toBe(String(at));
    expect((await claimOne()).jobId).toBe(overdue.id);
    expect(await queue.claim('another-worker', leaseMs)).toBeNull();
  });

  it('rejects ambiguous or overflowing schedules without creating a job', async () => {
    const id = randomUUID();
    const ambiguous = { ...job, id, runAt: 0, delayMs: 1 };
    // Calling from JavaScript or a request body can bypass the compile-time union.
    await expect(
      Reflect.apply(queue.enqueue, queue, [ambiguous]),
    ).rejects.toThrow(ZodError);
    await expect(
      queue.enqueue({ ...job, id, delayMs: Number.MAX_SAFE_INTEGER }),
    ).rejects.toThrow(RangeError);
    expect(await redis.exists(keys.job(id))).toBe(0);
    expect(await redis.zcard(keys.ready)).toBe(0);
    expect(await redis.zcard(keys.delayed)).toBe(0);
  });
});

describe('promoteDue', () => {
  it('skips future work and moves a due job once across competing calls', async () => {
    const future = await queue.enqueue({ ...job, delayMs: 60_000 });
    expect(await queue.promoteDue()).toBe(0);

    const due = await queue.enqueue({ ...job, delayMs: 100, priority: 'high' });
    expect(due.status).toBe('delayed');
    await waitUntil(due.runAt);
    const counts = await Promise.all([queue.promoteDue(), queue.promoteDue()]);
    expect(counts.sort()).toEqual([0, 1]);
    expect(await queue.promoteDue()).toBe(0);
    expect(await redis.zrange(keys.delayed, 0, -1)).toEqual([future.id]);
    expect(await redis.zscore(keys.ready, due.id)).toBe(
      String(readyScore('high', due.enqueuedAt)),
    );
    expect(await redis.hget(keys.job(due.id), 'status')).toBe('ready');
  });

  it('reports a due entry with a missing hash without moving it', async () => {
    const id = randomUUID();
    await redis.zadd(keys.delayed, 0, id);
    await expect(queue.promoteDue()).rejects.toThrow(QueueInvariantError);
    expect(await redis.zrange(keys.delayed, 0, -1)).toEqual([id]);
    expect(await redis.zcard(keys.ready)).toBe(0);
  });

  it('promotes an older scheduled job ahead of newer ready jobs of the same priority', async () => {
    const older = await queue.enqueue({ ...job, delayMs: 100 });
    await waitUntil(older.enqueuedAt + 1);
    const newer = await queue.enqueue(job);
    expect(newer.enqueuedAt).toBeGreaterThan(older.enqueuedAt);
    await waitUntil(older.runAt);
    const scriptKeys = [keys.job(older.id), keys.delayed, keys.ready];
    expect(
      await runScript(redis, scripts.promoteDue, scriptKeys, [older.id]),
    ).toEqual(['PROMOTED', expect.any(Number), 0]);
    expect(
      await runScript(redis, scripts.promoteDue, scriptKeys, [older.id]),
    ).toEqual(['SKIPPED']);
    expect(await redis.zrange(keys.ready, 0, -1)).toEqual([older.id, newer.id]);
  });
});

describe('claim', () => {
  it('takes the ready head by priority and returns a lease on Redis time', async () => {
    const low = await queue.enqueue({ ...job, priority: 'low' });
    const normal = await queue.enqueue(job);
    const high = await queue.enqueue({ ...job, priority: 'high' });
    const before = await redisTime();
    const first = await claimOne();
    expect(first).toMatchObject({
      jobId: high.id,
      workerId: 'worker-1',
      priority: 'high',
      fencingToken: '1',
      attempt: 1,
      maxAttempts: 3,
      type: job.type,
      payload: job.payload,
    });
    expect(first.leaseDeadline).toBeGreaterThanOrEqual(before + leaseMs);
    expect(first.leaseDeadline).toBeLessThanOrEqual(
      (await redisTime()) + leaseMs,
    );
    expect(await redis.zscore(keys.inFlight, high.id)).toBe(
      String(first.leaseDeadline),
    );
    expect((await claimOne()).jobId).toBe(normal.id);
    expect((await claimOne()).jobId).toBe(low.id);
    expect(await queue.claim('worker-1', leaseMs)).toBeNull();
    expect(await redis.hget(keys.job(high.id), 'attempts')).toBe('1');
  });

  it('returns the current head when an earlier read loses the race', async () => {
    const first = await queue.enqueue({ ...job, priority: 'high' });
    const next = await queue.enqueue(job);
    expect((await claimOne()).jobId).toBe(first.id);
    const reply = await runScript(
      redis,
      scripts.claim,
      [keys.job(first.id), keys.ready, keys.inFlight],
      [first.id, 'worker-2', randomUUID(), String(leaseMs)],
    );
    expect(reply).toEqual(['CONTENDED', next.id]);
    expect(await redis.zrange(keys.ready, 0, -1)).toEqual([next.id]);
  });

  it('repeating the same claim script arguments cannot increment the counters twice', async () => {
    const { id } = await queue.enqueue(job);
    const scriptKeys = [keys.job(id), keys.ready, keys.inFlight];
    const args = [id, 'worker-1', randomUUID(), String(leaseMs)];
    expect((await runScript(redis, scripts.claim, scriptKeys, args))[0]).toBe(
      'CLAIMED',
    );
    expect(await runScript(redis, scripts.claim, scriptKeys, args)).toEqual([
      'EMPTY',
    ]);
    expect(await redis.hmget(keys.job(id), 'attempts', 'fencingToken')).toEqual(
      ['1', '1'],
    );
  });

  it('gives each of 32 simultaneous workers a different job in one claim call', async () => {
    const count = 32;
    const jobs = await Promise.all(
      Array.from({ length: count }, () => queue.enqueue(job)),
    );
    const clients = Array.from({ length: count }, () =>
      createRedis(config.REDIS_URL, logger),
    );
    try {
      await Promise.all(clients.map((client) => client.connect()));
      const claims = await Promise.all(
        clients.map((client, index) =>
          new Queue(client, queueName).claim(`worker-${index}`, leaseMs),
        ),
      );
      const claimedIds = claims.flatMap((claim) =>
        claim ? [claim.jobId] : [],
      );
      expect(claimedIds).toHaveLength(count);
      expect(new Set(claimedIds).size).toBe(count);
      expect(new Set(claimedIds)).toEqual(new Set(jobs.map(({ id }) => id)));
      for (const [index, claim] of claims.entries()) {
        expect(claim).toMatchObject({
          workerId: `worker-${index}`,
          attempt: 1,
          fencingToken: '1',
        });
      }
      expect(await redis.zcard(keys.ready)).toBe(0);
      expect(await redis.zcard(keys.inFlight)).toBe(count);
    } finally {
      for (const client of clients) client.disconnect();
    }
  }, 10_000);

  it('reports a ready entry with a missing hash without mutating it', async () => {
    const id = randomUUID();
    await redis.zadd(keys.ready, 0, id);
    await expect(queue.claim('worker-1', leaseMs)).rejects.toThrow(
      QueueInvariantError,
    );
    expect(await redis.zrange(keys.ready, 0, -1)).toEqual([id]);
    expect(await redis.zcard(keys.inFlight)).toBe(0);
  });
});

describe('ack', () => {
  it('clears the lease and rejects a repeated acknowledgment', async () => {
    const { id } = await queue.enqueue(job);
    const claim = await claimOne();
    await queue.ack(claim);
    expect(await redis.zscore(keys.inFlight, id)).toBeNull();
    expect(
      await redis.hmget(
        keys.job(id),
        'status',
        'workerId',
        'claimId',
        'fencingToken',
      ),
    ).toEqual(['succeeded', null, null, '1']);
    await expect(queue.ack(claim)).rejects.toThrow(InvalidClaimError);
  });

  it('rejects stale tokens and claim IDs, preserving the full Redis counter', async () => {
    const { id } = await queue.enqueue(job);
    await redis.hset(keys.job(id), 'fencingToken', '9007199254740992');
    const claim = await claimOne();
    expect(claim.fencingToken).toBe('9007199254740993');
    await expect(
      queue.ack({ ...claim, fencingToken: '9007199254740992' }),
    ).rejects.toThrow(InvalidClaimError);
    await expect(
      queue.ack({ ...claim, claimId: randomUUID() }),
    ).rejects.toThrow(InvalidClaimError);
    await expect(
      queue.ack({ ...claim, workerId: 'other-worker' }),
    ).rejects.toThrow(InvalidClaimError);
    expect(await redis.zscore(keys.inFlight, id)).toBe(
      String(claim.leaseDeadline),
    );
    await queue.ack(claim);
  });

  it('rejects an expired owner before any reaper runs', async () => {
    const { id } = await queue.enqueue(job);
    const claim = await queue.claim('worker-1', 10);
    if (!claim) throw new Error('Expected a claim');
    await waitUntil(claim.leaseDeadline);
    await expect(queue.ack(claim)).rejects.toThrow(InvalidClaimError);
    expect(await redis.hget(keys.job(id), 'status')).toBe('in_flight');
    expect(await redis.zscore(keys.inFlight, id)).toBe(
      String(claim.leaseDeadline),
    );
  });

  it('returns a typed error for a missing job', async () => {
    await expect(
      queue.ack({
        jobId: randomUUID(),
        workerId: 'worker-1',
        claimId: randomUUID(),
        fencingToken: '1',
      }),
    ).rejects.toThrow(JobNotFoundError);
  });
});

describe('heartbeat', () => {
  it('extends the lease and a repeat with a shorter timeout cannot shorten it', async () => {
    const { id } = await queue.enqueue(job);
    const claim = await claimOne();
    const extended = await queue.heartbeat(claim, 2 * leaseMs);
    expect(extended).toBeGreaterThan(claim.leaseDeadline);
    expect(await queue.heartbeat(claim, 1)).toBe(extended);
    expect(await redis.zscore(keys.inFlight, id)).toBe(String(extended));
  });

  it('rejects a wrong token and a missing job without changing the lease', async () => {
    const { id } = await queue.enqueue(job);
    const claim = await claimOne();
    await expect(
      queue.heartbeat({ ...claim, fencingToken: '2' }, leaseMs),
    ).rejects.toBeInstanceOf(InvalidClaimError);
    await expect(
      queue.heartbeat({ ...claim, jobId: randomUUID() }, leaseMs),
    ).rejects.toBeInstanceOf(JobNotFoundError);
    expect(await redis.zscore(keys.inFlight, id)).toBe(
      String(claim.leaseDeadline),
    );
  });

  it('rejects an expired claim before the reaper runs', async () => {
    await queue.enqueue(job);
    const claim = await queue.claim('worker-1', 10);
    if (!claim) throw new Error('Expected a claim');
    await waitUntil(claim.leaseDeadline);
    await expect(queue.heartbeat(claim, leaseMs)).rejects.toBeInstanceOf(
      InvalidClaimError,
    );
    expect(await redis.zscore(keys.inFlight, claim.jobId)).toBe(
      String(claim.leaseDeadline),
    );
  });
});

describe('reap', () => {
  function reapCandidate(id: string) {
    return runScript(
      redis,
      scripts.reap,
      [keys.job(id), keys.inFlight, keys.delayed, keys.dead, keys.dlqArchive],
      [id, queueName, '0', '500', '30000', '250'],
    );
  }

  it('ignores a lease before its deadline', async () => {
    await queue.enqueue(job);
    const claim = await claimOne();
    expect(await reapCandidate(claim.jobId)).toEqual(['NOT_EXPIRED']);
    expect(await redis.zscore(keys.inFlight, claim.jobId)).toBe(
      String(claim.leaseDeadline),
    );
  });

  it('skips a renewed lease after its original deadline has passed', async () => {
    await queue.enqueue(job);
    const claim = await queue.claim('worker-1', 300);
    if (!claim) throw new Error('Expected a claim');
    const renewed = await queue.heartbeat(claim, leaseMs);
    await waitUntil(claim.leaseDeadline);
    expect(await reapCandidate(claim.jobId)).toEqual(['NOT_EXPIRED']);
    expect(await queue.reapExpired()).toEqual({ requeued: 0, dead: 0 });
    expect(await redis.zscore(keys.inFlight, claim.jobId)).toBe(
      String(renewed),
    );
  });

  it('rejects an expired candidate with the wrong hash state', async () => {
    const { id } = await queue.enqueue(job);
    await redis.zrem(keys.ready, id);
    await redis.zadd(keys.inFlight, 0, id);
    expect(await reapCandidate(id)).toEqual(['INCONSISTENT']);
    expect(await redis.zscore(keys.inFlight, id)).toBe('0');
    expect(await redis.hget(keys.job(id), 'status')).toBe('ready');
  });

  it('recovers an expired lease once under contention and fences the prior owner', async () => {
    const enqueued = await queue.enqueue(job);
    const first = await queue.claim('worker-1', 10);
    if (!first) throw new Error('Expected a claim');
    await waitUntil(first.leaseDeadline);
    const results = await Promise.all([
      queue.reapExpired(),
      queue.reapExpired(),
    ]);
    expect(results).toContainEqual({ requeued: 0, dead: 0 });
    expect(results).toContainEqual({ requeued: 1, dead: 0 });
    expect(await reapCandidate(enqueued.id)).toEqual(['SKIPPED']);
    const runAt = await redis.zscore(keys.delayed, enqueued.id);
    expect(Number(runAt)).toBeGreaterThanOrEqual(first.leaseDeadline + 1_000);
    expect(await queue.claim('worker-2', leaseMs)).toBeNull();
    await waitUntil(Number(runAt));
    expect(await queue.promoteDue()).toBe(1);
    expect(await redis.zscore(keys.ready, enqueued.id)).toBe(
      String(readyScore('normal', enqueued.enqueuedAt)),
    );

    const second = await queue.claim('worker-2', leaseMs);
    if (!second) throw new Error('Expected the recovered claim');
    expect(second).toMatchObject({
      jobId: enqueued.id,
      attempt: 2,
      fencingToken: '2',
    });
    await expect(queue.ack(first)).rejects.toBeInstanceOf(InvalidClaimError);
    await expect(queue.heartbeat(first, leaseMs)).rejects.toBeInstanceOf(
      InvalidClaimError,
    );
    await queue.ack(second);
  });

  it('moves an exhausted lease to dead and writes one archive snapshot on repeated reap', async () => {
    const enqueued = await queue.enqueue({ ...job, maxAttempts: 1 });
    const claim = await queue.claim('worker-1', 10);
    if (!claim) throw new Error('Expected a claim');
    await waitUntil(claim.leaseDeadline);
    expect(await queue.reapExpired()).toEqual({ requeued: 0, dead: 1 });
    expect(await reapCandidate(enqueued.id)).toEqual(['SKIPPED']);
    expect(await redis.zrange(keys.dead, 0, -1)).toEqual([enqueued.id]);
    expect(await redis.zcard(keys.inFlight)).toBe(0);
    expect(await redis.hgetall(keys.job(enqueued.id))).toMatchObject({
      status: 'dead',
      attempts: '1',
      lastError: 'Visibility lease expired',
    });
    const snapshots = await redis.xrange(keys.dlqArchive, '-', '+');
    expect(snapshots).toHaveLength(1);
    expect(snapshots[0]?.[1]).toEqual(
      expect.arrayContaining([
        'event',
        'dead',
        'jobId',
        enqueued.id,
        'queueName',
        queueName,
        'payload',
        JSON.stringify(job.payload),
      ]),
    );
    expect(await queue.claim('worker-2', leaseMs)).toBeNull();
  });

  it('surfaces a missing job hash while preserving the orphaned lease for repair', async () => {
    const id = randomUUID();
    await redis.zadd(keys.inFlight, 0, id);
    await expect(queue.reapExpired()).rejects.toThrow(QueueInvariantError);
    expect(await redis.zscore(keys.inFlight, id)).toBe('0');
    expect(await redis.zcard(keys.ready)).toBe(0);
    expect(await redis.zcard(keys.dead)).toBe(0);
  });
});

describe('fail and replay', () => {
  it('calculates exact backoff and jitter boundaries without mutating Redis state', async () => {
    const source = await readFile(
      new URL('../../src/lua/retry.lua', import.meta.url),
      'utf8',
    );
    const result = await redis.eval(
      `${source}\nreturn {retryDelay(1, 50, 500, 19, 0),
      retryDelay(1, 50, 500, 19, 0.999), retryDelay(2, 50, 500, 0, 0.5),
      retryDelay(3, 50, 410, 19, 0.999), retryDelay(2147483647, 50, 500, 19, 0.999)}`,
      0,
    );
    expect(result).toEqual([100, 119, 200, 410, 500]);
  });

  it('supplies default timing for a pre-Phase 4 hash while preserving its attempt budget', async () => {
    const { id } = await queue.enqueue({ ...job, maxAttempts: 2 });
    await redis.hdel(keys.job(id), 'baseDelayMs', 'maxDelayMs', 'jitterMs');
    const claim = await claimOne();
    const consumer = new Queue(redis, queueName, { random: () => 0 });
    const before = await redisTime();
    expect(claim.maxAttempts).toBe(2);
    const result = await consumer.fail(claim, 'legacy job');
    if (result.status !== 'delayed') throw new Error('Expected a retry');
    expect(result.runAt).toBeGreaterThanOrEqual(before + 1_000);
    expect(result.runAt).toBeLessThanOrEqual((await redisTime()) + 1_000);
  });
  it('uses the saved per-type exponential backoff, jitter and cap before archiving exhaustion', async () => {
    const producer = new Queue(redis, queueName, {
      retryPolicies: {
        test: {
          maxAttempts: 3,
          baseDelayMs: 30,
          maxDelayMs: 100,
          jitterMs: 19,
        },
      },
    });
    const consumer = new Queue(redis, queueName, { random: () => 0.5 });
    const { id } = await producer.enqueue(job);
    for (const [attempt, delay] of [
      [1, 70],
      [2, 100],
    ] as const) {
      const claim = await consumer.claim('worker-1', leaseMs);
      if (!claim) throw new Error('Expected a claim');
      expect(claim).toMatchObject({ jobId: id, attempt, maxAttempts: 3 });
      const before = await redisTime();
      const result = await consumer.fail(claim, new Error('retry me'));
      expect(result.status).toBe('delayed');
      if (result.status !== 'delayed') throw new Error('Expected a retry');
      expect(result.runAt).toBeGreaterThanOrEqual(before + delay);
      expect(result.runAt).toBeLessThanOrEqual((await redisTime()) + delay);
      expect(await redis.zscore(keys.inFlight, id)).toBeNull();
      await expect(consumer.fail(claim, 'duplicate')).rejects.toBeInstanceOf(
        InvalidClaimError,
      );
      await waitUntil(result.runAt);
      expect(await consumer.promoteDue()).toBe(1);
    }

    const claim = await claimOne();
    const error = new Error('permanent failure');
    expect(await consumer.fail(claim, error)).toEqual({ status: 'dead' });
    await expect(consumer.fail(claim, error)).rejects.toBeInstanceOf(
      InvalidClaimError,
    );
    expect(await redis.xlen(keys.dlqArchive)).toBe(1);
    expect(await consumer.listDead()).toEqual([
      {
        id,
        type: job.type,
        status: 'dead',
        attempts: 3,
        lastError: error.message,
        stack: error.stack,
        payload: job.payload,
        replayCount: 0,
        deadAt: expect.any(Number),
      },
    ]);
  });

  it('rejects missing jobs, wrong credentials and expired owners on fail', async () => {
    const { id } = await queue.enqueue(job);
    const claim = await queue.claim('worker-1', 200);
    if (!claim) throw new Error('Expected a claim');
    await expect(
      queue.fail({ ...claim, jobId: randomUUID() }, 'missing'),
    ).rejects.toBeInstanceOf(JobNotFoundError);
    await expect(
      queue.fail({ ...claim, fencingToken: '2' }, 'stale'),
    ).rejects.toBeInstanceOf(InvalidClaimError);
    await expect(
      queue.fail({ ...claim, claimId: randomUUID() }, 'stale'),
    ).rejects.toBeInstanceOf(InvalidClaimError);
    await waitUntil(claim.leaseDeadline);
    await expect(queue.fail(claim, 'expired')).rejects.toBeInstanceOf(
      InvalidClaimError,
    );
    expect(await redis.hget(keys.job(id), 'status')).toBe('in_flight');
    expect(await redis.zcard(keys.delayed)).toBe(0);
    expect(await redis.xlen(keys.dlqArchive)).toBe(0);
  });

  it('replays once per dead generation and preserves fencing across repeated exhaustion', async () => {
    const { id } = await queue.enqueue({ ...job, maxAttempts: 1 });
    await expect(queue.replay({ id, replayCount: 0 })).rejects.toBeInstanceOf(
      ReplayConflictError,
    );
    const first = await claimOne();
    await queue.fail(first, 'first');
    const schedule = { id, replayCount: 0 };
    const races = await Promise.allSettled([
      queue.replay(schedule),
      queue.replay(schedule),
    ]);
    expect(races.filter((result) => result.status === 'fulfilled')).toEqual([
      { status: 'fulfilled', value: { id, replayCount: 1 } },
    ]);
    expect(races.filter((result) => result.status === 'rejected')).toEqual([
      { status: 'rejected', reason: expect.any(ReplayConflictError) },
    ]);
    expect(await queue.listDead()).toEqual([]);
    expect(
      await redis.hmget(
        keys.job(id),
        'status',
        'attempts',
        'lastError',
        'fencingToken',
      ),
    ).toEqual(['ready', '0', null, '1']);
    const second = await claimOne();
    expect(second).toMatchObject({ jobId: id, attempt: 1, fencingToken: '2' });
    await expect(queue.fail(first, 'zombie')).rejects.toBeInstanceOf(
      InvalidClaimError,
    );
    await queue.fail(second, 'second');
    await expect(queue.replay(schedule)).rejects.toBeInstanceOf(
      ReplayConflictError,
    );
    expect(await queue.listDead()).toMatchObject([
      { id, replayCount: 1, lastError: 'second' },
    ]);
    expect(await redis.xlen(keys.dlqArchive)).toBe(3);
  });

  it('returns mixed bulk outcomes and rejects missing jobs individually', async () => {
    const missing = randomUUID();
    await expect(
      queue.replay({ id: missing, replayCount: 0 }),
    ).rejects.toBeInstanceOf(JobNotFoundError);
    const first = await queue.enqueue({ ...job, maxAttempts: 1 });
    await queue.fail(await claimOne(), 'failed');
    const second = await queue.enqueue({ ...job, maxAttempts: 1 });
    await queue.fail(await claimOne(), 'failed');
    const page = await queue.listDead(0, 1);
    expect(page).toHaveLength(1);
    expect((await queue.listDead(1, 1))[0]?.id).not.toBe(page[0]?.id);
    const results = await queue.replayMany([
      { id: first.id, replayCount: 0 },
      { id: missing, replayCount: 0 },
      { id: second.id, replayCount: 0 },
    ]);
    expect(results).toEqual([
      { id: first.id, replayCount: 1, status: 'replayed' },
      { id: missing, status: 'failed', error: expect.any(JobNotFoundError) },
      { id: second.id, replayCount: 1, status: 'replayed' },
    ]);
    expect(await redis.zcard(keys.ready)).toBe(2);
    expect(await queue.listDead()).toEqual([]);
  });
});
