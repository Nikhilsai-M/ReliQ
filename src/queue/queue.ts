import { randomUUID } from 'node:crypto';
import type { Redis } from 'ioredis';
import {
  InvalidClaimError,
  JobExistsError,
  JobNotFoundError,
  QueueInvariantError,
} from './errors.js';
import { queueKeys } from './keys.js';
import { priorityBandMs, readyScore } from './priority.js';
import {
  claimCredential,
  claimInput,
  claimReply,
  enqueueInput,
  enqueueReply,
  jsonPayload,
  promoteLimit,
} from './schemas.js';
import type { ClaimCredential, EnqueueInput } from './schemas.js';
import { runScript, scripts } from './scripts.js';

export type { ClaimCredential, EnqueueInput } from './schemas.js';

export class Queue {
  private readonly keys: ReturnType<typeof queueKeys>;

  constructor(
    private readonly redis: Redis,
    name: string,
  ) {
    this.keys = queueKeys(name);
  }

  async enqueue(input: EnqueueInput) {
    const job = enqueueInput.parse(input);
    const id = job.id ?? randomUUID();
    const reply = await runScript(
      this.redis,
      scripts.enqueue,
      [this.keys.job(id), this.keys.ready, this.keys.delayed],
      [
        id,
        job.type,
        JSON.stringify(job.payload),
        job.priority,
        String(readyScore(job.priority, 0)),
        String(priorityBandMs),
        String(job.maxAttempts),
        job.runAt === undefined ? 'delay' : 'absolute',
        String(job.runAt ?? job.delayMs ?? 0),
      ],
    );

    if (reply[0] === 'JOB_EXISTS') throw new JobExistsError(id);
    if (reply[0] === 'INVALID_TIME')
      throw new RangeError('Scheduled time exceeds the safe integer range');
    if (reply[0] !== 'ENQUEUED')
      throw new QueueInvariantError(`Enqueue ${id}: ${reply[0]}`);
    const [, status, enqueuedAt, runAt] = enqueueReply.parse(reply);
    return { id, status, enqueuedAt, runAt };
  }

  async promoteDue(limit = 100): Promise<number> {
    const ids = await this.redis.zrange(
      this.keys.delayed,
      0,
      promoteLimit.parse(limit) - 1,
    );
    let promoted = 0;
    for (const id of ids) {
      const [status] = await runScript(
        this.redis,
        scripts.promoteDue,
        [this.keys.job(id), this.keys.delayed, this.keys.ready],
        [id],
      );
      if (status === 'NOT_DUE') break;
      if (status === 'SKIPPED') continue;
      if (status !== 'PROMOTED')
        throw new QueueInvariantError(`Promote ${id}: ${status}`);
      promoted++;
    }
    return promoted;
  }

  async claim(workerId: string, visibilityTimeoutMs: number) {
    claimInput.parse({ workerId, visibilityTimeoutMs });
    const claimId = randomUUID();
    let [jobId] = await this.redis.zrange(this.keys.ready, 0, 0);

    while (jobId !== undefined) {
      const reply = await runScript(
        this.redis,
        scripts.claim,
        [this.keys.job(jobId), this.keys.ready, this.keys.inFlight],
        [jobId, workerId, claimId, String(visibilityTimeoutMs)],
      );
      if (reply[0] === 'EMPTY') return null;
      if (reply[0] === 'CONTENDED' && typeof reply[1] === 'string') {
        // The script already observed the new head. Skip another read or poll delay.
        jobId = reply[1];
        continue;
      }
      if (reply[0] === 'INVALID_TIME')
        throw new RangeError('Lease deadline exceeds the safe integer range');
      if (reply[0] !== 'CLAIMED')
        throw new QueueInvariantError(`Claim ${jobId}: ${reply[0]}`);

      const [
        ,
        type,
        payloadJson,
        priority,
        attempt,
        maxAttempts,
        fencingToken,
        leaseDeadline,
      ] = claimReply.parse(reply);
      const payload = jsonPayload.parse(JSON.parse(payloadJson));
      return {
        jobId,
        workerId,
        claimId,
        fencingToken,
        type,
        payload,
        priority,
        attempt,
        maxAttempts,
        leaseDeadline,
      };
    }
    return null;
  }

  async ack(credential: ClaimCredential): Promise<void> {
    const { jobId, workerId, claimId, fencingToken } =
      claimCredential.parse(credential);
    const [status] = await runScript(
      this.redis,
      scripts.ack,
      [this.keys.job(jobId), this.keys.inFlight],
      [jobId, workerId, claimId, fencingToken],
    );
    if (status === 'JOB_MISSING') throw new JobNotFoundError(jobId);
    if (status === 'INVALID_CLAIM') throw new InvalidClaimError(jobId);
    if (status !== 'ACKED')
      throw new QueueInvariantError(`Ack ${jobId}: ${status}`);
  }
}
