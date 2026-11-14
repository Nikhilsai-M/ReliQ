import { z } from 'zod';
import { priorities } from './priority.js';
import { maxAttemptCount as attemptCount } from './retry-policy.js';

const jobId = z.uuid();
export const timestamp = z.int().nonnegative();
const workerId = z.string().min(1);
export const queueName = z.string().regex(/^[A-Za-z0-9_-]+$/);
const fencingToken = z.string().regex(/^[1-9][0-9]*$/);

type Json = string | number | boolean | null | Json[] | { [key: string]: Json };

function isJson(value: unknown): value is Json {
  if (value === null || typeof value === 'string' || typeof value === 'boolean')
    return true;
  if (typeof value === 'number') return Number.isFinite(value);
  if (Array.isArray(value)) return value.every(isJson);
  if (typeof value !== 'object') return false;
  const prototype: unknown = Object.getPrototypeOf(value);
  return (
    (prototype === Object.prototype || prototype === null) &&
    Object.values(value).every(isJson)
  );
}

// Validate without copying the object so arbitrary JSON property names round-trip.
export const jsonPayload = z.custom<Json>(isJson, 'Expected a JSON payload');

const job = z.object({
  id: jobId.optional(),
  type: z.string().min(1),
  payload: jsonPayload,
  priority: z.enum(priorities).default('normal'),
  maxAttempts: attemptCount.optional(),
});

export const enqueueInput = z.union([
  job.extend({ runAt: timestamp.optional(), delayMs: z.never().optional() }),
  job.extend({ runAt: z.never().optional(), delayMs: timestamp }),
]);
export type EnqueueInput = z.input<typeof enqueueInput>;

export const claimInput = z.object({
  workerId,
  visibilityTimeoutMs: z.int().positive(),
});

export const claimCredential = z.object({
  jobId,
  workerId,
  claimId: z.uuid(),
  fencingToken,
});
export type ClaimCredential = z.infer<typeof claimCredential>;

export const promoteLimit = z.int().min(1).max(1_000);
export const replaySelection = z.object({
  id: jobId,
  replayCount: z.int().nonnegative().max(2_147_483_647),
});
export const dlqEntry = z.object({
  type: z.string(),
  payload: z.string(),
  lastError: z.string(),
  stack: z.string().optional(),
  attempts: z.coerce.number().pipe(attemptCount),
  deadAt: z.coerce.number().pipe(timestamp),
  replayCount: z.coerce.number().pipe(replaySelection.shape.replayCount),
  status: z.literal('dead'),
});
export const heartbeatReply = z.tuple([z.literal('EXTENDED'), timestamp]);
export const promoteReply = z.tuple([
  z.literal('PROMOTED'),
  timestamp,
  timestamp,
]);
export const ackReply = z.tuple([z.literal('ACKED'), timestamp, attemptCount]);
export const failureReply = z.tuple([
  z.literal('FAILED'),
  z.enum(['delayed', 'dead']),
  timestamp,
  timestamp,
  attemptCount,
  workerId,
]);
export const replayReply = z.tuple([
  z.literal('REPLAYED'),
  timestamp,
  timestamp,
  attemptCount,
]);

export const enqueueReply = z.tuple([
  z.literal('ENQUEUED'),
  z.enum(['ready', 'delayed']),
  timestamp,
  timestamp,
]);

export const claimReply = z.tuple([
  z.literal('CLAIMED'),
  z.string(),
  z.string(),
  z.enum(priorities),
  attemptCount,
  attemptCount,
  fencingToken,
  timestamp,
  timestamp,
]);
