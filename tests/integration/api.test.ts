import { execFile } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtemp, unlink, rmdir, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { afterAll, afterEach, beforeAll, beforeEach, expect, it } from 'vitest';
import { z } from 'zod';
import { createApp } from '../../src/api/app.js';
import { loadConfig } from '../../src/config/env.js';
import { createLogger } from '../../src/config/logger.js';
import { queueKeys } from '../../src/queue/keys.js';
import { Queue } from '../../src/queue/queue.js';
import { createRedis } from '../../src/queue/redis.js';
import { runScript, scripts } from '../../src/queue/scripts.js';

const logger = createLogger('silent');
const redis = createRedis(loadConfig().REDIS_URL, logger);
const server = createServer(
  createApp((name) => new Queue(redis, name), logger),
);
const exec = promisify(execFile);
let base: string;
let name: string;
let queue: Queue;
let keys: ReturnType<typeof queueKeys>;
const jobId = z.object({ id: z.uuid() });

beforeAll(async () => {
  await redis.connect();
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string')
    throw new Error('Expected TCP address');
  base = `http://127.0.0.1:${address.port}`;
});
afterAll(async () => {
  redis.disconnect();
  await new Promise<void>((resolve) => server.close(() => resolve()));
});
beforeEach(() => {
  name = `api-${randomUUID()}`;
  queue = new Queue(redis, name);
  keys = queueKeys(name);
});
afterEach(async () => {
  let cursor = '0';
  do {
    const [next, matches] = await redis.scan(
      cursor,
      'MATCH',
      `reliq:{${name}}:*`,
    );
    if (matches.length > 0) await redis.unlink(...matches);
    cursor = next;
  } while (cursor !== '0');
});

function request(path: string, method = 'GET', body?: unknown) {
  return fetch(`${base}/queues/${name}${path}`, {
    method,
    ...(body === undefined
      ? {}
      : {
          body: JSON.stringify(body),
          headers: { 'content-type': 'application/json' },
        }),
  });
}

async function rejected(response: Response, status: number, code: string) {
  expect(response.status).toBe(status);
  expect(await response.json()).toMatchObject({
    error: { code, message: expect.any(String) },
  });
}

async function deadJob() {
  const { id } = await queue.enqueue({
    type: 'sample',
    payload: null,
    maxAttempts: 1,
  });
  const claim = await queue.claim('worker-terminal', 30_000);
  if (!claim || claim.jobId !== id)
    throw new Error('Expected the terminal job');
  await queue.fail(claim, 'terminal error');
  return id;
}

it('rechecks in-flight membership and reports an orphaned lease as an internal error', async () => {
  const { id } = await queue.enqueue({ type: 'sample', payload: null });
  const claim = await queue.claim('worker-live', 30_000);
  if (!claim) throw new Error('Expected claim');
  const inspect = () =>
    runScript(
      redis,
      scripts.inspectInFlight,
      [keys.job(id), keys.inFlight],
      [id],
    );
  expect((await inspect())[0]).toBe('IN_FLIGHT');
  expect((await inspect())[0]).toBe('IN_FLIGHT');
  await queue.ack(claim);
  expect(await inspect()).toEqual(['SKIPPED']);
  await redis.del(keys.job(id));
  await redis.zadd(keys.inFlight, 0, id);
  await rejected(await request('/in-flight'), 500, 'INTERNAL_ERROR');
  expect(await redis.zscore(keys.inFlight, id)).toBe('0');
});

it('enqueues over HTTP and reports each live depth and in-flight owner', async () => {
  const dead = await deadJob();
  const immediate = await request('/jobs', 'POST', {
    type: 'sample',
    payload: { message: 'test' },
  });
  expect(immediate.status).toBe(201);
  const { id } = jobId.parse(await immediate.json());
  const claim = await queue.claim('worker-live', 10_000);
  expect(claim?.jobId).toBe(id);
  expect(
    (
      await request('/jobs', 'POST', {
        type: 'sample',
        payload: null,
        delayMs: 60_000,
      })
    ).status,
  ).toBe(201);
  await queue.enqueue({ type: 'sample', payload: null });
  const stats = await request('/stats');
  expect(stats.status).toBe(200);
  expect(stats.headers.get('cache-control')).toBe('no-store');
  expect(await stats.json()).toEqual({
    queue: name,
    depth: { ready: 1, delayed: 1, inFlight: 1, dead: 1 },
  });
  const flight = await request('/in-flight?limit=1');
  const result = z
    .object({
      jobs: z.array(
        z.object({
          id: z.uuid(),
          workerId: z.string(),
          leaseRemainingMs: z.number(),
        }),
      ),
    })
    .parse(await flight.json());
  expect(result.jobs).toEqual([
    { id, workerId: 'worker-live', leaseRemainingMs: expect.any(Number) },
  ]);
  expect(result.jobs[0]?.leaseRemainingMs).toBeGreaterThan(0);
  expect(result.jobs[0]?.leaseRemainingMs).toBeLessThanOrEqual(10_000);
  const [seconds, microseconds] = await redis.time();
  await redis.zadd(
    keys.inFlight,
    Number(seconds) * 1_000 + Math.floor(Number(microseconds) / 1_000) - 1,
    id,
  );
  expect(await (await request('/in-flight')).json()).toMatchObject({
    jobs: [{ id, leaseRemainingMs: 0 }],
  });
  expect(await (await request('/in-flight?offset=1')).json()).toMatchObject({
    jobs: [],
  });
  expect(await (await request('/dlq?limit=1')).json()).toMatchObject({
    jobs: [{ id: dead, replayCount: 0 }],
  });
});

it('validates JSON, pagination and job IDs with a consistent error envelope', async () => {
  const id = randomUUID();
  const data = { id, type: 'test', payload: 0 };
  await rejected(
    await request('/jobs', 'POST', { ...data, delayMs: 5, runAt: 1 }),
    400,
    'VALIDATION_ERROR',
  );
  await rejected(await request('/dlq?limit=0'), 400, 'VALIDATION_ERROR');
  await rejected(
    await fetch(`${base}/queues/%ZZ/stats`),
    400,
    'VALIDATION_ERROR',
  );
  await rejected(
    await request('/dlq?offset=0&offset=1'),
    400,
    'VALIDATION_ERROR',
  );
  await rejected(
    await request('/dlq/bad-id/replay', 'POST', { replayCount: 0 }),
    400,
    'VALIDATION_ERROR',
  );
  await rejected(await request('/jobs', 'POST'), 415, 'UNSUPPORTED_MEDIA_TYPE');
  const malformed = await fetch(`${base}/queues/${name}/jobs`, {
    method: 'POST',
    body: '{',
    headers: { 'content-type': 'application/json' },
  });
  await rejected(malformed, 400, 'INVALID_JSON');
  await rejected(
    await request('/jobs', 'POST', {
      type: 'test',
      payload: 'a'.repeat(256 * 1024),
    }),
    413,
    'BODY_TOO_LARGE',
  );
  expect((await request('/jobs', 'POST', data)).status).toBe(201);
  await rejected(await request('/jobs', 'POST', data), 409, 'JOB_EXISTS');
  await rejected(await request('/unknown'), 404, 'ROUTE_NOT_FOUND');
});

it('replays one or many dead letters and purges only the selected generation', async () => {
  // Create all DLQ entries before replay releases ready jobs.
  const single = await deadJob();
  const bulk = await deadJob();
  const purged = await deadJob();
  const missing = randomUUID();
  const replayed = await request(`/dlq/${single}/replay`, 'POST', {
    replayCount: 0,
  });
  expect(replayed.status).toBe(200);
  expect(await replayed.json()).toEqual({ id: single, replayCount: 1 });
  await rejected(
    await request(`/dlq/${single}/replay`, 'POST', { replayCount: 0 }),
    409,
    'DEAD_GENERATION_CONFLICT',
  );

  const batch = await request('/dlq/replay', 'POST', {
    jobs: [
      { id: bulk, replayCount: 0 },
      { id: missing, replayCount: 0 },
    ],
  });
  expect(batch.status).toBe(200);
  expect(await batch.json()).toMatchObject({
    results: [
      { id: bulk, replayCount: 1, status: 'replayed' },
      { id: missing, status: 'failed', error: { code: 'JOB_NOT_FOUND' } },
    ],
  });
  await rejected(
    await request(`/dlq/${purged}`, 'DELETE', { replayCount: 1 }),
    409,
    'DEAD_GENERATION_CONFLICT',
  );
  const archiveBefore = await redis.xrange(keys.dlqArchive, '-', '+');
  const response = await request(`/dlq/${purged}`, 'DELETE', {
    replayCount: 0,
  });
  expect(response.status).toBe(204);
  expect(await response.text()).toBe('');
  expect(await redis.exists(keys.job(purged))).toBe(0);
  expect(await redis.xrange(keys.dlqArchive, '-', '+')).toEqual(archiveBefore);
  await rejected(
    await request(`/dlq/${purged}`, 'DELETE', { replayCount: 0 }),
    404,
    'JOB_NOT_FOUND',
  );
  await rejected(
    await request('/jobs', 'POST', { id: purged, type: 'test', payload: null }),
    409,
    'JOB_EXISTS',
  );
});

it('keeps a generation safe when replay and purge race', async () => {
  const id = await deadJob();
  const [replay, purge] = await Promise.all([
    request(`/dlq/${id}/replay`, 'POST', { replayCount: 0 }),
    request(`/dlq/${id}`, 'DELETE', { replayCount: 0 }),
  ]);
  const pair = [replay.status, purge.status];
  expect([
    [200, 409],
    [404, 204],
  ]).toContainEqual(pair);
  expect(await queue.listDead()).toEqual([]);
  expect(await redis.hget(keys.job(id), 'status')).toBe(
    replay.ok ? 'ready' : null,
  );
});

it('executes the compiled CLI against the live API and exits nonzero on failures', async () => {
  const id = await deadJob();
  const dir = await mkdtemp(join(tmpdir(), 'reliq-cli-'));
  const file = join(dir, 'job.json');
  const cli = (command: string, ...args: string[]) =>
    exec(process.execPath, [
      'dist/src/cli/cli.js',
      command,
      name,
      '--url',
      base,
      ...args,
    ]);
  try {
    await writeFile(
      file,
      JSON.stringify({ type: 'test', payload: true, delayMs: 60_000 }),
    );
    expect(
      JSON.parse((await cli('enqueue', '--file', file)).stdout),
    ).toMatchObject({ status: 'delayed' });
    expect(JSON.parse((await cli('stats')).stdout)).toMatchObject({
      depth: { delayed: 1, dead: 1 },
    });
    expect(JSON.parse((await cli('dlq', '--limit', '1')).stdout)).toMatchObject(
      { jobs: [{ id }] },
    );
    expect(JSON.parse((await cli('in-flight')).stdout)).toMatchObject({
      jobs: [],
    });
    await expect(
      cli('replay', '--id', id, '--generation', '1'),
    ).rejects.toMatchObject({
      code: 1,
      stderr: expect.stringContaining('DEAD_GENERATION_CONFLICT'),
    });
    await writeFile(
      file,
      JSON.stringify({
        jobs: [
          { id, replayCount: 0 },
          { id: randomUUID(), replayCount: 0 },
        ],
      }),
    );
    await expect(cli('replay-bulk', '--file', file)).rejects.toMatchObject({
      code: 1,
      stdout: expect.stringContaining('replayed'),
    });
    const claim = await queue.claim('cli-test', 30_000);
    if (!claim) throw new Error('Expected replayed job');
    await queue.fail(claim, 'failed again');
    expect(
      JSON.parse((await cli('purge', '--id', id, '--generation', '1')).stdout),
    ).toEqual({ purged: id });
    await expect(
      cli('purge', '--id', id, '--generation', '1'),
    ).rejects.toMatchObject({
      code: 1,
      stderr: expect.stringContaining('JOB_NOT_FOUND'),
    });
  } finally {
    await unlink(file).catch((error: NodeJS.ErrnoException) => {
      if (error.code !== 'ENOENT') throw error;
    });
    await rmdir(dir);
  }
}, 10_000);
