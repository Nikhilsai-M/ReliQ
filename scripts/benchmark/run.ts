import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { cpus, release, totalmem } from 'node:os';
import { setTimeout } from 'node:timers/promises';
import { z } from 'zod';
import { loadConfig } from '../../src/config/env.js';
import { createLogger } from '../../src/config/logger.js';
import { Queue } from '../../src/queue/queue.js';
import { createRedis } from '../../src/queue/redis.js';

const [workers, jobs] = z
  .tuple([
    z.coerce.number().int().min(1).max(64),
    z.coerce.number().int().min(1).max(100_000),
  ])
  .parse([process.argv[2] ?? '1', process.argv[3] ?? '2000']);
const durationMs = 5;
const batchSize = 32;
const name = `benchmark-${randomUUID()}`;
const redis = createRedis(loadConfig().REDIS_URL, createLogger('silent'));
const queue = new Queue(redis, name);
const enqueued = new Map<string, number>();
const acknowledged = new Map<string, number>();
const failures: Error[] = [];
let ready = 0;
let executions = 0;
let duplicateAcks = 0;
let lastAck = 0;

const message = z.union([
  z.object({ ready: z.literal(true) }),
  z.object({ jobId: z.uuid(), at: z.number() }),
  z.object({ executions: z.int().nonnegative() }),
]);
const fleet = Array.from({ length: workers }, (_, index) => {
  const child = fork(
    'dist/scripts/benchmark/worker.js',
    [name, `worker-${index + 1}`],
    { execArgv: [], stdio: ['ignore', 'ignore', 'inherit', 'ipc'] },
  );
  child.once('error', (error) => failures.push(error));
  child.on('message', (raw) => {
    const result = message.safeParse(raw);
    if (!result.success) return;
    const event = result.data;
    if ('ready' in event) ready++;
    else if ('executions' in event) executions += event.executions;
    else {
      if (acknowledged.has(event.jobId)) duplicateAcks++;
      acknowledged.set(event.jobId, event.at);
      lastAck = performance.now();
    }
  });
  return {
    child,
    closed: new Promise<number | null>((resolve) =>
      child.once('close', resolve),
    ),
  };
});

async function waitFor(predicate: () => boolean, timeout: number) {
  const until = performance.now() + timeout;
  while (!predicate() && performance.now() < until) {
    if (failures[0]) throw failures[0];
    if (
      fleet.some(
        ({ child }) => child.exitCode !== null || child.signalCode !== null,
      )
    )
      throw new Error('Benchmark worker exited prematurely');
    await setTimeout(10);
  }
  assert(predicate(), 'Benchmark timed out');
}

try {
  await redis.connect();
  await waitFor(() => ready === workers, 15_000);
  const started = performance.now();
  for (let offset = 0; offset < jobs; offset += batchSize) {
    const batch = Array.from(
      { length: Math.min(batchSize, jobs - offset) },
      () => ({ id: randomUUID(), type: 'sleep', payload: { durationMs } }),
    );
    const results = await Promise.all(batch.map((job) => queue.enqueue(job)));
    for (const { id, enqueuedAt } of results) enqueued.set(id, enqueuedAt);
  }
  await waitFor(() => acknowledged.size === jobs, 120_000);
  for (const { child } of fleet) child.send('stop');
  assert(
    (await Promise.all(fleet.map(({ closed }) => closed))).every(
      (code) => code === 0,
    ),
  );

  const latencies = Array.from(enqueued, ([id, at]) => {
    const completedAt = acknowledged.get(id);
    assert(completedAt !== undefined && completedAt >= at);
    return completedAt - at;
  }).sort((a, b) => a - b);
  const percentile = (p: number) =>
    latencies[Math.ceil(latencies.length * p) - 1];
  const elapsedMs = lastAck - started;
  assert.equal(executions, jobs);
  assert.equal(duplicateAcks, 0);
  console.log(
    JSON.stringify({
      workers,
      jobs,
      completed: acknowledged.size,
      lost: jobs - acknowledged.size,
      handlerExecutions: executions,
      duplicateAcks,
      durationMs,
      batchSize,
      elapsedMs,
      jobsPerSecond: (jobs * 1000) / elapsedMs,
      p50Ms: percentile(0.5),
      p95Ms: percentile(0.95),
      p99Ms: percentile(0.99),
      runtime: {
        node: process.version,
        kernel: release(),
        cpu: cpus()[0]?.model,
        logicalCpuCount: cpus().length,
        ramBytes: totalmem(),
      },
    }),
  );
} finally {
  for (const { child } of fleet)
    if (child.exitCode === null && child.signalCode === null)
      child.kill('SIGKILL');
  await Promise.all(fleet.map(({ closed }) => closed));
  try {
    if (redis.status === 'ready') {
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
    }
  } finally {
    redis.disconnect();
  }
}
