import { z } from 'zod';
import { retryPolicies } from '../queue/retry-policy.js';

const envSchema = z.object({
  REDIS_URL: z.url({ protocol: /^rediss?$/, hostname: /.+/ }),
  DATABASE_URL: z.url({ protocol: /^postgres(?:ql)?$/, hostname: /.+/ }),
  LOG_LEVEL: z
    .enum(['trace', 'debug', 'info', 'warn', 'error', 'fatal', 'silent'])
    .default('info'),
  JOB_RETRY_POLICIES: z
    .string()
    .transform((raw, ctx): unknown => {
      try {
        return JSON.parse(raw || '{}');
      } catch {
        ctx.addIssue({ code: 'custom', message: 'Expected a JSON object' });
        return z.NEVER;
      }
    })
    .pipe(retryPolicies)
    .prefault('{}'),
});

export type Config = z.infer<typeof envSchema>;

export class ConfigError extends Error {
  constructor(fields: string[]) {
    super(`Invalid environment variables: ${fields.join(', ')}`);
    this.name = 'ConfigError';
  }
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const config = envSchema.safeParse(env);
  if (!config.success) {
    // Report setting names only so malformed URLs cannot leak credentials.
    throw new ConfigError(
      config.error.issues.map((issue) => issue.path.join('.')),
    );
  }
  return config.data;
}
