import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { setTimeout } from 'node:timers/promises';
import { afterAll, afterEach, beforeAll, beforeEach, expect, it } from 'vitest';
import { loadConfig } from '../../src/config/env.js';
import { createLogger } from '../../src/config/logger.js';
import { queueKeys } from '../../src/queue/keys.js';
import { Queue } from '../../src/queue/queue.js';
import { createRedis } from '../../src/queue/redis.js';
import { Worker } from '../../src/worker/worker.js';
import type { Handler } from '../../src/worker/worker.js';

const config = loadConfig();
const logger = createLogger('silent');
const redis = createRedis(config.REDIS_URL, logger);
let name: string;
let queue: Queue;
let keys: ReturnType<typeof queueKeys>;
let jobIds: string[];

beforeAll(() => redis.connect());
afterAll(() => redis.disconnect());
beforeEach(() => {
  name = `worker-test-${randomUUID()}`;
  queue = new Queue(redis, name);
  keys = queueKeys(name);
  jobIds = [];
});
afterEach(async () => {
  await redis.unlink(
    keys.ready,
    keys.delayed,
    keys.inFlight,
    keys.dead,
    keys.dlqArchive,
    ...jobIds.map(keys.job),
  );
});

async function enqueue(durationMs: number, delayMs = 0) {
  const { id } = await queue.enqueue({
    type: 'sleep',
    payload: { durationMs },
    delayMs,
  });
  jobIds.push(id);
  return id;
}

function status(id: string) {
  return redis.hget(keys.job(id), 'status');
}

function worker(handler: Handler, drainTimeoutMs = 2_000) {
  return new Worker(
    queue,
    new Map([['sleep', handler]]),
    {
      workerId: 'test-worker',
      visibilityTimeoutMs: 300,
      heartbeatIntervalMs: 50,
      pollIntervalMs: 20,
      maintenanceIntervalMs: 30,
      drainTimeoutMs,
    },
    logger,
  );
}

function gate() {
  let open!: () => void;
  const wait = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { open, wait };
}

it('promotes due jobs and reaps abandoned claims while a handler keeps its lease', async () => {
  const activeId = await enqueue(0);
  const active = gate();
  const release = gate();
  const instance = worker(async (job) => {
    if (job.jobId === activeId) {
      active.open();
      await release.wait;
    }
  });
  const running = instance.run();
  try {
    await active.wait;
    const firstDeadline = Number(await redis.zscore(keys.inFlight, activeId));
    const abandonedId = await enqueue(0);
    expect((await queue.claim('abandoned-worker', 50))?.jobId).toBe(
      abandonedId,
    );
    const delayedId = await enqueue(0, 100);
    await expect.poll(() => status(abandonedId)).toBe('ready');
    await expect.poll(() => status(delayedId)).toBe('ready');
    await expect
      .poll(async () => Number(await redis.zscore(keys.inFlight, activeId)))
      .toBeGreaterThan(firstDeadline);
    expect(await redis.hget(keys.job(activeId), 'attempts')).toBe('1');
    release.open();
    await expect.poll(() => status(activeId)).toBe('succeeded');
    await expect.poll(() => status(abandonedId)).toBe('succeeded');
    await expect.poll(() => status(delayedId)).toBe('succeeded');
  } finally {
    release.open();
    await instance.drain();
    await running;
  }
});

it('signals an active handler when lease ownership is lost', async () => {
  const id = await enqueue(0);
  let cancelled = false;
  const instance = worker(async (_job, signal) => {
    try {
      await setTimeout(3_000, undefined, { signal });
    } finally {
      cancelled = signal.aborted;
    }
  });
  const running = instance.run();
  try {
    await expect.poll(() => status(id)).toBe('in_flight');
    await redis.hset(keys.job(id), 'fencingToken', '2');
    await expect.poll(() => cancelled).toBe(true);
    expect(await status(id)).toBe('in_flight');
  } finally {
    await instance.drain();
    await running;
  }
});

function startProcess(drainMs: number) {
  const child = spawn(process.execPath, ['dist/scripts/worker.js'], {
    env: {
      ...process.env,
      QUEUE_NAME: name,
      WORKER_ID: 'shutdown-test',
      LOG_LEVEL: 'info',
      VISIBILITY_TIMEOUT_MS: '350',
      HEARTBEAT_INTERVAL_MS: '60',
      POLL_INTERVAL_MS: '20',
      MAINTENANCE_INTERVAL_MS: '30',
      DRAIN_TIMEOUT_MS: String(drainMs),
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout.on('data', (chunk: Buffer) => {
    output += chunk.toString();
  });
  child.stderr.on('data', (chunk: Buffer) => {
    output += chunk.toString();
  });
  const exit = new Promise<{
    code: number | null;
    signal: NodeJS.Signals | null;
  }>((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code, signal) => resolve({ code, signal }));
  });
  return { child, exit, logs: () => output };
}

// Windows terminates child processes on SIGTERM rather than delivering a POSIX signal.
it.skipIf(process.platform === 'win32')(
  'on SIGTERM, keeps renewing the active job, acknowledges it and leaves the next job ready',
  async () => {
    const id = await enqueue(1_000);
    const next = await enqueue(0, 60_000);
    const process = startProcess(2_000);
    try {
      await expect.poll(() => status(id)).toBe('in_flight');
      const ready = await enqueue(0);
      expect(process.child.kill('SIGTERM')).toBe(true);
      expect(await process.exit, process.logs()).toEqual({
        code: 0,
        signal: null,
      });
      expect(process.logs()).toContain('Worker drained');
      expect(await status(id)).toBe('succeeded');
      expect(await status(ready)).toBe('ready');
      expect(await status(next)).toBe('delayed');
    } finally {
      if (process.child.exitCode === null) process.child.kill('SIGKILL');
      await process.exit;
    }
  },
);

it.skipIf(process.platform === 'win32')(
  'on drain timeout, exits and leaves its lease recoverable by another worker',
  async () => {
    const id = await enqueue(3_000);
    const process = startProcess(100);
    try {
      await expect.poll(() => status(id)).toBe('in_flight');
      expect(process.child.kill('SIGTERM')).toBe(true);
      expect(await process.exit, process.logs()).toEqual({
        code: 1,
        signal: null,
      });
      expect(process.logs()).toContain('Worker drain timed out');
      expect(await status(id)).toBe('in_flight');
      await expect
        .poll(() => queue.reapExpired(), { timeout: 2_000 })
        .toEqual({ requeued: 1, dead: 0 });
      const claim = await queue.claim('replacement', 1_000);
      expect(claim).toMatchObject({ jobId: id, attempt: 2, fencingToken: '2' });
    } finally {
      if (process.child.exitCode === null) process.child.kill('SIGKILL');
      await process.exit;
    }
  },
);
