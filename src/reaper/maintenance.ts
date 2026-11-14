import { setTimeout } from 'node:timers/promises';
import type { Logger } from 'pino';
import type { Queue } from '../queue/queue.js';

export async function maintainQueue(
  queue: Queue,
  intervalMs: number,
  signal: AbortSignal,
  logger: Logger,
) {
  while (!signal.aborted) {
    const [promotion, reaping] = await Promise.allSettled([
      queue.promoteDue(),
      queue.reapExpired(),
    ]);
    if (promotion.status === 'rejected')
      logger.warn({ err: promotion.reason }, 'Promotion failed');
    if (reaping.status === 'rejected')
      logger.warn({ err: reaping.reason }, 'Reaping failed');
    else if (reaping.value.requeued + reaping.value.dead > 0)
      logger.info(reaping.value, 'Expired leases recovered');

    try {
      await setTimeout(intervalMs, undefined, { signal });
    } catch (error) {
      if (!signal.aborted) throw error;
    }
  }
}
