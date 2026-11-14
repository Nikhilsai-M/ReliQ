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
import { JobExistsError } from '../../src/queue/errors.js';
import { createRedis } from '../../src/queue/redis.js';
import { isolatedCompose } from './docker.js';
import { measure } from './measure.js';
import { classifyRepeats, workerMessage } from './trace.js';

const [scenario, seed, total] = z
  .tuple([
    z.enum(['graceful', 'hard', 'continuous']),
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
const trace: (Extract<z.infer<typeof workerMessage>, { claimId: string }> & {
  workerId: string;
})[] = [];
const scenarioLabel =
  scenario === 'hard'
    ? 'Redis process SIGKILL'
    : scenario === 'continuous'
      ? 'Graceful Redis restart with continuous enqueues'
      : 'Graceful Redis restart';
const enqueueStats = {
  attempts: 0,
  failedAttempts: 0,
  retries: 0,
  jobsRetried: 0,
  reconciledReplies: 0,
};
let firstEnqueueMs: number | undefined;
let lastEnqueueMs: number | undefined;

function elapsed() {
  return Math.round(performance.now() - started);
}

function log(event: string, details: object = {}) {
  const entry = {
    event,
    at: new Date().toISOString(),
    elapsedMs: elapsed(),
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
    const parsed = workerMessage.safeParse(raw);
    if (!parsed.success) return;
    if (parsed.data.event === 'ready') ready = true;
    else if (parsed.data.event === 'debit_committed') debitCommitted = true;
    else {
      trace.push({ ...parsed.data, workerId });
      if (parsed.data.event === 'completed') completed.add(parsed.data.jobId);
    }
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
  scenarioLabel,
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
    [source, destination, total * 2],
  );
  const payload = {
    debitAccountId: source,
    creditAccountId: destination,
    amountCents: 1,
  };
  async function enqueue(input: EnqueueInput, retry = false) {
    const id = randomUUID();
    const until = performance.now() + 30_000;
    let failures = 0;
    let reconciled = false;
    firstEnqueueMs ??= elapsed();
    for (;;) {
      enqueueStats.attempts++;
      if (failures > 0) enqueueStats.retries++;
      try {
        await queue.enqueue({ ...input, id });
        break;
      } catch (error) {
        // A retry using this generated ID can find the original committed write after a lost reply.
        if (error instanceof JobExistsError && failures > 0) {
          reconciled = true;
          enqueueStats.reconciledReplies++;
          break;
        }
        enqueueStats.failedAttempts++;
        failures++;
        if (!retry || performance.now() >= until) throw error;
        await setTimeout(10);
      }
    }
    if (failures > 0) enqueueStats.jobsRetried++;
    acknowledged.push(id);
    lastEnqueueMs = elapsed();
    await journal.write(
      `${JSON.stringify({ id, acknowledgedAt: new Date().toISOString(), elapsedMs: lastEnqueueMs, attempts: failures + 1, reconciled })}\n`,
    );
    return { id };
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
  const workerKillMs = elapsed();

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
  const produceTail = async (retry: boolean) => {
    for (let index = 0; index < tailSize; index++) {
      await enqueue({ type: 'transfer', payload }, retry);
      if (retry) await setTimeout(10);
    }
  };
  if (scenario !== 'continuous') await produceTail(false);
  const restartStart = Date.now();
  const redisCommandMs = elapsed();
  const acknowledgedBeforeRedis = acknowledged.length;
  log(scenario === 'hard' ? 'redis_sigkill' : 'redis_docker_restart', {
    acknowledged: acknowledged.length,
    completedSoFar: completed.size,
    recentEnqueues: tailSize,
  });
  let redisReadyMs = 0;
  let restartEnd = 0;
  let acknowledgedAtRedisReady = 0;
  const restart = async () => {
    if (scenario !== 'hard') await container.restart();
    else await container.kill();
    await container.start();
    await waitFor(
      () => redis?.status === 'ready',
      15_000,
      'the producer to reconnect',
    );
    restartEnd = Date.now();
    redisReadyMs = elapsed();
    acknowledgedAtRedisReady = acknowledged.length;
    log('redis_ready', { acknowledged: acknowledged.length });
  };
  await Promise.all([
    restart(),
    scenario === 'continuous' ? produceTail(true) : undefined,
  ]);
  await journal.sync();

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
  const duplicates = stats.handler_executions - stats.jobs_entered;
  const acknowledgedSet = new Set(acknowledged);
  const repeatCauses = classifyRepeats(
    trace.filter(({ jobId }) => acknowledgedSet.has(jobId)),
    { jobId: pauseJob.id, workerId: victimId },
    { start: restartStart, end: restartEnd },
    duplicates,
  );
  const report = {
    scenario,
    scenarioLabel,
    seed,
    workers,
    acknowledged: acknowledged.length,
    completed: acknowledged.length - lost.length,
    redisSucceeded: succeeded.size,
    receipts: receipts.size,
    lost: lost.length,
    ...counts,
    duplicateHandlerExecutions: duplicates,
    repeatCauses: repeatCauses.counts,
    enqueueStats,
    timeline: {
      firstEnqueueMs,
      workerKillMs,
      redisCommandMs,
      redisReadyMs,
      lastEnqueueMs,
      acknowledgedBeforeRedis,
      acknowledgedAtRedisReady,
      finalAcknowledged: acknowledged.length,
    },
    drainReached: drained,
  };
  log('result', report);
  await writeFile(
    join(output, 'attempts.ndjson'),
    trace.map((entry) => JSON.stringify(entry)).join('\n') + '\n',
  );
  await writeFile(
    join(output, 'repeats.ndjson'),
    repeatCauses.repeats.map((entry) => JSON.stringify(entry)).join('\n') +
      '\n',
  );
  await writeFile(
    join(output, 'lost.ndjson'),
    lost.map((id) => JSON.stringify({ id })).join('\n'),
  );
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
  assert.equal(stats.balances[source], total * 2 - stats.debits);
  assert.equal(stats.balances[destination], stats.credits);
  assert.equal(receipts.size, stats.receipt_effects);
  if (scenario === 'continuous')
    assert(
      enqueueStats.failedAttempts > 0 && enqueueStats.retries > 0,
      'The producer must experience and retry the outage',
    );
  if (scenario !== 'hard') {
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
