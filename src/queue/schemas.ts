import { z } from 'zod';
import { priorities } from './priority.js';

const jobId = z.uuid();
const timestamp = z.int().nonnegative();
const workerId = z.string().min(1);
const attemptCount = z.int().positive().max(2_147_483_647);
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
  maxAttempts: attemptCount.default(3),
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
]);
