import assert from 'node:assert/strict';
import { fork, type ChildProcess } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import { mkdir, open, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { join, resolve } from 'node:path';
import { setTimeout } from 'node:timers/promises';
import { z } from 'zod';
import { createLogger } from '../../src/config/logger.js';
import { migrate } from '../../src/db/migrate.js';
import { createPostgresPool } from '../../src/db/pool.js';
import { queueKeys } from '../../src/queue/keys.js';
import { Queue, type EnqueueInput } from '../../src/queue/queue.js';
import { createRedis } from '../../src/queue/redis.js';
import { isolatedCompose } from './docker.js';
import { measure } from './measure.js';

const [scenario, seed, total] = z
  .tuple([
    z.enum(['graceful', 'hard']),
    z.coerce.number().int().min(1).max(0xffff_ffff),
    z.coerce.number().int().min(10).max(100_000),
  ])
  .parse([
    process.argv[2],
    process.argv[3] ?? '20261115',
    process.argv[4] ?? '10000',
  ]);
const workers = 5;
const tailSize = Math.min(100, Math.floor(total / 2));
const logger = createLogger('silent');
const project = `reliq-chaos-${randomBytes(6).toString('hex')}`;
const output = resolve('chaos-results', `${scenario}-${seed}-${project}`);

async function reservePort() {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert(address && typeof address !== 'string');
  return {
    port: address.port,
    release: () =>
      new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

// Keep each chosen host port fixed across Docker stop/start so clients reconnect to the same URL.
const [redisPort, postgresPort] = await Promise.all([
  reservePort(),
  reservePort(),
]);
const compose = isolatedCompose(project, {
  redis: redisPort.port,
  postgres: postgresPort.port,
});
await Promise.all([redisPort.release(), postgresPort.release()]);
const started = performance.now();
const children = new Set<ChildProcess>();
const completed = new Set<string>();
const acknowledged: string[] = [];
const events: object[] = [];

function log(event: string, details: object = {}) {
  const entry = {
    event,
    at: new Date().toISOString(),
    elapsedMs: Math.round(performance.now() - started),
    ...details,
  };
  events.push(entry);
  console.log(JSON.stringify(entry));
}

// The seed fixes kill selection and the delay after the debit checkpoint.
let state = seed;
function sample() {
  state ^= state << 13;
  state ^= state >>> 17;
  state ^= state << 5;
  return (state >>> 0) / 0x1_0000_0000;
}

const message = z.discriminatedUnion('event', [
  z.object({ event: z.literal('ready') }),
  z.object({ event: z.literal('debit_committed'), jobId: z.uuid() }),
  z.object({ event: z.literal('completed'), jobId: z.uuid() }),
]);

function launch(env: NodeJS.ProcessEnv, workerId: string, pauseId?: string) {
  const child = fork(
    'dist/scripts/chaos/worker.js',
    [project, workerId, ...(pauseId ? [pauseId] : [])],
    {
      env,
      stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
    },
  );
  children.add(child);
  let ready = false;
  let debitCommitted = false;
  child.on('message', (raw) => {
    const parsed = message.safeParse(raw);
    if (!parsed.success) return;
    if (parsed.data.event === 'ready') ready = true;
    else if (parsed.data.event === 'debit_committed') debitCommitted = true;
    else completed.add(parsed.data.jobId);
  });
  const exited = new Promise<void>((resolve, reject) => {
    child.once('error', reject);
    child.once('exit', () => {
      children.delete(child);
      resolve();
    });
  });
  return {
    child,
    exited,
    isReady: () => ready,
    debitCommitted: () => debitCommitted,
  };
}

async function waitFor(
  check: () => boolean,
  timeout: number,
  description: string,
) {
  const end = performance.now() + timeout;
  while (!check() && performance.now() < end) await setTimeout(50);
  assert(check(), `Timed out waiting for ${description}`);
}

async function persistedCompleted(
  redis: ReturnType<typeof createRedis>,
  ids: string[],
) {
  const keys = queueKeys(project);
  const succeeded = new Set<string>();
  for (let offset = 0; offset < ids.length; offset += 500) {
    const batch = ids.slice(offset, offset + 500);
    const pipeline = redis.pipeline();
    for (const id of batch) pipeline.hget(keys.job(id), 'status');
    const results = await pipeline.exec();
    assert(results);
    for (const [index, id] of batch.entries()) {
      const response = results[index];
      assert(response && !response[0], 'Could not read final Redis state');
      if (response[1] === 'succeeded') succeeded.add(id);
    }
  }
  return succeeded;
}

await mkdir(output, { recursive: true });
const journal = await open(join(output, 'acknowledged.ndjson'), 'wx');
log('scenario_started', {
  scenario,
  seed,
  requestedJobs: total,
  workers,
  project,
  output,
});
let redis: ReturnType<typeof createRedis> | undefined;
let pool: ReturnType<typeof createPostgresPool> | undefined;
try {
  await compose.start();
  const container = await compose.redisContainer();
  const urls = await compose.urls();
  const env = {
    ...process.env,
    REDIS_URL: urls.redis,
    DATABASE_URL: urls.postgres,
    LOG_LEVEL: 'silent',
  };
  pool = createPostgresPool(urls.postgres, logger);
  await migrate(pool, resolve('src/db/migrations'));
  redis = createRedis(urls.redis, logger);
  await redis.connect();
  const queue = new Queue(redis, project, {
    retryPolicies: {
      transfer: {
        maxAttempts: 8,
        baseDelayMs: 20,
        maxDelayMs: 100,
        jitterMs: 20,
      },
    },
  });
  const source = randomUUID();
  const destination = randomUUID();
  await pool.query(
    'INSERT INTO accounts (id, balance_cents) VALUES ($1, $3), ($2, 0)',
    [source, destination, total],
  );
  const payload = {
    debitAccountId: source,
    creditAccountId: destination,
    amountCents: 1,
  };
  async function enqueue(input: EnqueueInput) {
    const job = await queue.enqueue(input);
    acknowledged.push(job.id);
    await journal.write(
      `${JSON.stringify({ id: job.id, acknowledgedAt: new Date().toISOString() })}\n`,
    );
    return job;
  }

  // Seeded worker selection is assigned the only available job before the other workers start.
  const victimId = `worker-${1 + Math.floor(sample() * workers)}`;
  const killDelayMs = Math.floor(sample() * 51);
  const pauseJob = await enqueue({
    type: 'transfer',
    priority: 'high',
    payload,
  });
  const victim = launch(env, victimId, pauseJob.id);
  await waitFor(victim.debitCommitted, 15_000, 'the victim to commit debit');
  log('debit_checkpoint', {
    workerId: victimId,
    jobId: pauseJob.id,
    killDelayMs,
  });
  assert.equal(
    (
      await pool.query(
        'SELECT step_key FROM transfer_effects WHERE job_id = $1',
        [pauseJob.id],
      )
    ).rows[0]?.step_key,
    'debit',
  );
  assert.equal(
    (
      await pool.query('SELECT 1 FROM receipts WHERE job_id = $1', [
        pauseJob.id,
      ])
    ).rowCount,
    0,
  );
  await setTimeout(killDelayMs);
  log('worker_sigkill', {
    workerId: victimId,
    pid: victim.child.pid,
    jobId: pauseJob.id,
  });
  assert(victim.child.kill('SIGKILL'));
  await victim.exited;

  for (let index = 1; index < total - tailSize; index++)
    await enqueue({ type: 'transfer', payload });
  const fleet = Array.from({ length: workers }, (_, index) =>
    launch(env, `replacement-${index + 1}`),
  );
  await waitFor(
    () => fleet.every((worker) => worker.isReady()),
    15_000,
    'the five replacement workers',
  );
  for (let index = 0; index < tailSize; index++)
    await enqueue({ type: 'transfer', payload });
  log(scenario === 'graceful' ? 'redis_docker_restart' : 'redis_sigkill', {
    acknowledged: acknowledged.length,
    completedSoFar: completed.size,
    recentEnqueues: tailSize,
  });
  if (scenario === 'graceful') await container.restart();
  else await container.kill();
  await journal.sync();
  await container.start();
  await waitFor(
    () => redis?.status === 'ready',
    15_000,
    'the producer to reconnect',
  );
  log('redis_ready');

  // An empty live queue can mean either success or AOF loss. Final states decide which IDs completed.
  const end = performance.now() + 120_000;
  let drained = false;
  while (performance.now() < end) {
    const { ready, delayed, inFlight } = await queue.depth();
    if (ready + delayed + inFlight === 0) {
      drained = true;
      break;
    }
    await setTimeout(200);
  }
  const succeeded = await persistedCompleted(redis, acknowledged);
  const observed = new Set([...succeeded, ...completed]);
  const stats = await measure(pool, acknowledged, source, destination);
  const lost = acknowledged.filter(
    (id) => !observed.has(id) || !stats.receipts.has(id),
  );
  const { receipts, ...counts } = stats;
  const report = {
    scenario,
    seed,
    workers,
    acknowledged: acknowledged.length,
    completed: acknowledged.length - lost.length,
    redisSucceeded: succeeded.size,
    receipts: receipts.size,
    lost: lost.length,
    ...counts,
    duplicateHandlerExecutions: stats.handler_executions - stats.jobs_entered,
    drainReached: drained,
    lostIds: lost,
  };
  log('result', { ...report, lostIds: undefined });
  await writeFile(
    join(output, 'summary.json'),
    JSON.stringify(report, null, 2) + '\n',
  );
  assert.equal(acknowledged.length, total);
  assert(drained, 'The queue did not drain within the observation window');
  assert(
    observed.has(pauseJob.id) && receipts.has(pauseJob.id),
    'The mid-debit crash job did not recover',
  );
  assert(
    stats.handler_executions > stats.jobs_entered,
    'Expected a duplicate handler execution after killing the worker',
  );
  assert.equal(stats.duplicated_effects, 0, 'Duplicate committed side effects');
  assert.equal(stats.balances[source], total - stats.debits);
  assert.equal(stats.balances[destination], stats.credits);
  assert.equal(receipts.size, stats.receipt_effects);
  if (scenario === 'graceful') {
    assert.equal(lost.length, 0, 'Graceful restart lost acknowledged jobs');
    assert.equal(
      stats.side_effects,
      3 * total,
      'Expected all three committed effects for every job',
    );
  }
} finally {
  await journal.close();
  for (const child of children) child.kill('SIGKILL');
  redis?.disconnect();
  await pool?.end();
  await writeFile(
    join(output, 'events.ndjson'),
    events.map((event) => JSON.stringify(event)).join('\n') + '\n',
  );
  await compose.stop();
}
