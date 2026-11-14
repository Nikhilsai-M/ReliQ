import { z } from 'zod';
import { loadConfig } from './env.js';
import { queueName } from '../queue/schemas.js';

const apiEnv = z.object({
  API_PORT: z.coerce.number().int().min(1).max(65_535).default(3000),
});
const serverEnv = z.object({
  API_HOST: z.enum(['127.0.0.1', '0.0.0.0']).default('127.0.0.1'),
  API_QUEUES: z
    .string()
    .transform((value) => value.split(',').map((name) => name.trim()))
    .pipe(z.array(queueName).min(1))
    .prefault('default'),
});

export function loadApiConfig(env: NodeJS.ProcessEnv = process.env) {
  return {
    ...loadConfig(env),
    ...serverEnv.parse(env),
    API_PORT: loadApiPort(env),
  };
}

export function loadApiPort(env: NodeJS.ProcessEnv = process.env) {
  return apiEnv.parse(env).API_PORT;
}
