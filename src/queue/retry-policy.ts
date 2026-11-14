import { z } from 'zod';

export const maxAttemptCount = z.int().positive().max(2_147_483_647);
const durationMs = z.int().nonnegative().max(2_147_483_647);

export const retryPolicy = z
  .object({
    maxAttempts: maxAttemptCount.default(3),
    baseDelayMs: durationMs.min(1).default(500),
    maxDelayMs: durationMs.min(1).default(30_000),
    jitterMs: durationMs.default(250),
  })
  .strict()
  .refine((policy) => policy.baseDelayMs <= policy.maxDelayMs, {
    path: ['maxDelayMs'],
    message: 'Maximum delay must be at least the base delay',
  });

export const retryPolicies = z.record(z.string().min(1), retryPolicy);
export const defaultRetryPolicy = retryPolicy.parse({});
export type RetryPolicies = z.infer<typeof retryPolicies>;
