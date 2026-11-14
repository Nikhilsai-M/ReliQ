import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, expect, it } from 'vitest';
import { loadConfig } from '../../src/config/env.js';
import { createLogger } from '../../src/config/logger.js';
import { InvalidClaimError } from '../../src/queue/errors.js';
import { queueKeys } from '../../src/queue/keys.js';
import { Queue } from '../../src/queue/queue.js';
import { createRedis } from '../../src/queue/redis.js';
import { runScript, scripts } from '../../src/queue/scripts.js';

const redis = createRedis(loadConfig().REDIS_URL, createLogger('silent'));
const name = `mutation-${randomUUID()}`;
const queue = new Queue(redis, name);
const keys = queueKeys(name);
const ids: string[] = [];
beforeAll(() => redis.connect());
afterAll(async () => {
  try {
    await redis.unlink(
      keys.inFlight,
      keys.ready,
      keys.delayed,
      keys.dead,
      keys.dlqArchive,
      ...ids.map(keys.job),
    );
  } finally {
    redis.disconnect();
  }
});

async function claim() {
  const { id } = await queue.enqueue({ type: 'test', payload: null });
  ids.push(id);
  const claim = await queue.claim('owner', 30_000);
  if (!claim) throw new Error('Expected claim');
  return claim;
}

it('fencing prevents a stale acknowledgment from marking unfinished work succeeded', async () => {
  const current = await claim();
  // Keep the current nonce and worker valid to isolate the token comparison.
  await expect(
    queue.ack({ ...current, fencingToken: '9' }),
  ).rejects.toBeInstanceOf(InvalidClaimError);
  expect(await redis.hget(keys.job(current.jobId), 'status')).toBe('in_flight');
});

it('reap refuses an active candidate before its current deadline', async () => {
  const { jobId } = await claim();
  // Pass a candidate directly to isolate the Lua recheck from the earlier scan.
  const result = await runScript(
    redis,
    scripts.reap,
    [keys.job(jobId), keys.inFlight, keys.delayed, keys.dead, keys.dlqArchive],
    [jobId, name, '0', '500', '30000', '250'],
  );
  expect(result).toEqual(['NOT_EXPIRED']);
  expect(await redis.hget(keys.job(jobId), 'status')).toBe('in_flight');
});
