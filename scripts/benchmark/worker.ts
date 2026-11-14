import { loadConfig } from '../../src/config/env.js';
import { createLogger } from '../../src/config/logger.js';
import { sleep } from '../../src/jobs/sleep.js';
import { Queue } from '../../src/queue/queue.js';
import { createRedis } from '../../src/queue/redis.js';
import { Worker } from '../../src/worker/worker.js';

const [name, workerId] = process.argv.slice(2);
if (!name || !workerId || !process.send)
  throw new Error('Start this worker through the benchmark');
const logger = createLogger('silent');
const redis = createRedis(loadConfig().REDIS_URL, logger);
const queue = new Queue(redis, name, {
  record(event) {
    if (event.to === 'succeeded')
      process.send?.({ jobId: event.jobId, at: event.at });
  },
});
let executions = 0;
const worker = new Worker(
  queue,
  new Map([
    [
      'sleep',
      async (job, signal) => {
        executions++;
        await sleep(job, signal);
      },
    ],
  ]),
  {
    workerId,
    visibilityTimeoutMs: 30_000,
    heartbeatIntervalMs: 10_000,
    pollIntervalMs: 10,
    maintenanceIntervalMs: 500,
    drainTimeoutMs: 5_000,
  },
  logger,
);

process.on('message', (message: unknown) => {
  if (message === 'stop') void worker.drain();
});
process.once('disconnect', () => {
  void worker.drain();
});
try {
  await redis.connect();
  process.send({ ready: true });
  await worker.run();
  if (process.connected) process.send({ executions });
} finally {
  redis.disconnect();
  if (process.connected) process.disconnect();
}
