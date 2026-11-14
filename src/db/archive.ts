import { setTimeout } from 'node:timers/promises';
import type { Redis } from 'ioredis';
import type { Pool } from 'pg';
import type { Logger } from 'pino';
import { z } from 'zod';
import { queueKeys } from '../queue/keys.js';
import { timestamp } from '../queue/schemas.js';
import { maxAttemptCount } from '../queue/retry-policy.js';

const redisInteger = z.string().regex(/^\d+$/).transform(Number);
const shared = {
  queueName: z.string(),
  jobId: z.uuid(),
  replayCount: redisInteger.pipe(z.int().nonnegative().max(2_147_483_647)),
};
const archiveEvent = z.discriminatedUnion('event', [
  z.object({
    ...shared,
    event: z.literal('dead'),
    type: z.string(),
    payload: z.string(),
    lastError: z.string(),
    stack: z.string().optional(),
    attempts: redisInteger.pipe(maxAttemptCount),
    deadAt: redisInteger.pipe(timestamp),
  }),
  z.object({
    ...shared,
    event: z.literal('replay'),
    replayedAt: redisInteger.pipe(timestamp),
  }),
]);

export class DeadLetterArchive {
  private readonly stream: string;

  constructor(
    private readonly redis: Redis,
    private readonly pool: Pool,
    queueName: string,
    private readonly logger: Logger,
  ) {
    this.stream = queueKeys(queueName).dlqArchive;
  }

  async flush(signal?: AbortSignal): Promise<number> {
    const entries = await this.redis.xrange(this.stream, '-', '+', 'COUNT', 50);
    let flushed = 0;
    for (const [id, fields] of entries) {
      if (signal?.aborted) break;
      const pairs = [];
      for (let index = 0; index < fields.length; index += 2) {
        pairs.push([fields[index], fields[index + 1]]);
      }
      const event = archiveEvent.parse(Object.fromEntries(pairs));
      if (event.event === 'dead') {
        await this.pool.query(
          `
          INSERT INTO dead_letter (job_id, replay_count, queue_name, job_type, payload, last_error, stack, attempts, dead_at)
          VALUES ($1, $2, $3, $4, $5::json, $6, $7, $8, $9)
          ON CONFLICT (job_id, replay_count) DO NOTHING`,
          [
            event.jobId,
            event.replayCount,
            event.queueName,
            event.type.replaceAll('\0', '\\0'),
            event.payload,
            event.lastError,
            event.stack || null,
            event.attempts,
            new Date(event.deadAt),
          ],
        );
      } else {
        const result = await this.pool.query(
          `UPDATE dead_letter SET replayed_at = COALESCE(replayed_at, $1)
          WHERE job_id = $2 AND replay_count = $3 AND queue_name = $4`,
          [
            new Date(event.replayedAt),
            event.jobId,
            event.replayCount,
            event.queueName,
          ],
        );
        if (result.rowCount === 0)
          throw new Error(`Missing dead-letter archive for job ${event.jobId}`);
      }
      // Each writer waits for its own durable insert or update. Duplicate writers can safely race.
      await this.redis.xdel(this.stream, id);
      flushed++;
    }
    return flushed;
  }

  async run(signal: AbortSignal): Promise<void> {
    let delay = 250;
    while (!signal.aborted) {
      try {
        const count = await this.flush(signal);
        delay = 250;
        if (count > 0) continue;
      } catch (err) {
        delay = Math.min(delay * 2, 5_000);
        this.logger.warn(
          { err, delay },
          'Dead-letter archive write failed; retry scheduled',
        );
      }
      try {
        await setTimeout(delay, undefined, { signal });
      } catch (err) {
        if (!signal.aborted) throw err;
      }
    }
  }
}
