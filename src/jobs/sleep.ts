import { setTimeout } from 'node:timers/promises';
import { z } from 'zod';
import type { Handler } from '../worker/worker.js';

const sleepPayload = z.object({
  durationMs: z.int().nonnegative().max(2_147_483_647),
});

export const sleep: Handler = async (job, signal) => {
  const { durationMs } = sleepPayload.parse(job.payload);
  await setTimeout(durationMs, undefined, { signal });
};
