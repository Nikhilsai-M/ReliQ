import { z } from 'zod';
import { queueName } from '../queue/schemas.js';
import { loadConfig } from './env.js';

const milliseconds = z.coerce.number().int().positive().max(2_147_483_647);
const workerEnv = z
  .object({
    QUEUE_NAME: queueName.default('default'),
    WORKER_ID: z.string().min(1).optional(),
    VISIBILITY_TIMEOUT_MS: milliseconds.default(30_000),
    HEARTBEAT_INTERVAL_MS: milliseconds.default(10_000),
    POLL_INTERVAL_MS: milliseconds.default(200),
    MAINTENANCE_INTERVAL_MS: milliseconds.default(500),
    DRAIN_TIMEOUT_MS: milliseconds.default(30_000),
  })
  .refine(
    (config) => config.HEARTBEAT_INTERVAL_MS < config.VISIBILITY_TIMEOUT_MS,
    {
      path: ['HEARTBEAT_INTERVAL_MS'],
      message: 'Heartbeat interval must be shorter than the visibility timeout',
    },
  );

export function loadWorkerConfig(env: NodeJS.ProcessEnv = process.env) {
  return { ...loadConfig(env), ...workerEnv.parse(env) };
}
