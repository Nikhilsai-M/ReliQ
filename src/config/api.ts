import { z } from 'zod';
import { loadConfig } from './env.js';

const apiEnv = z.object({
  API_PORT: z.coerce.number().int().min(1).max(65_535).default(3000),
});

export function loadApiConfig(env: NodeJS.ProcessEnv = process.env) {
  return { ...loadConfig(env), API_PORT: loadApiPort(env) };
}

export function loadApiPort(env: NodeJS.ProcessEnv = process.env) {
  return apiEnv.parse(env).API_PORT;
}
