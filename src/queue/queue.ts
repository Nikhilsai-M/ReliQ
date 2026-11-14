import { randomUUID } from 'node:crypto';
import type { Redis } from 'ioredis';
import {
  InvalidClaimError,
  DeadGenerationConflictError,
  JobExistsError,
  JobNotFoundError,
  QueueInvariantError,
  ReplayConflictError,
} from './errors.js';
import { queueKeys } from './keys.js';
import type { LedgerEvent } from './ledger-event.js';
import { priorityBandMs, readyScore } from './priority.js';
import { defaultRetryPolicy } from './retry-policy.js';
import type { RetryPolicies } from './retry-policy.js';
import {
  ackReply,
  claimCredential,
  claimInput,
  claimReply,
  enqueueInput,
  enqueueReply,
  failureReply,
  heartbeatReply,
  inFlightReply,
  jsonPayload,
  promoteLimit,
  promoteReply,
  purgeReply,
  queueDepthReply,
  dlqEntry,
  replayReply,
  replaySelection,
  timestamp,
} from './schemas.js';
import type { ClaimCredential, EnqueueInput } from './schemas.js';
import { runScript, scripts } from './scripts.js';
import { z } from 'zod';

export type { ClaimCredential, EnqueueInput } from './schemas.js';
export type ReplaySelection = z.input<typeof replaySelection>;

type QueueOptions = {
  retryPolicies?: RetryPolicies;
  random?: () => number;
  record?: (event: LedgerEvent) => void;
};

const fallbackRetry = [
  defaultRetryPolicy.baseDelayMs,
  defaultRetryPolicy.maxDelayMs,
  defaultRetryPolicy.jitterMs,
].map(String);

export class Queue {
  private readonly keys: ReturnType<typeof queueKeys>;

  constructor(
    private readonly redis: Redis,
    private readonly name: string,
    private readonly options: QueueOptions = {},
  ) {
    this.keys = queueKeys(name);
  }

  async enqueue(input: EnqueueInput) {
    const job = enqueueInput.parse(input);
    const id = job.id ?? randomUUID();
    const policies = this.options.retryPolicies;
    const policy =
      (policies && Object.hasOwn(policies, job.type)
        ? policies[job.type]
        : undefined) ?? defaultRetryPolicy;
    const maxAttempts = job.maxAttempts ?? policy.maxAttempts;
    const reply = await runScript(
      this.redis,
      scripts.enqueue,
      [this.keys.job(id), this.keys.ready, this.keys.delayed, this.keys.purged],
      [
        id,
        job.type,
        JSON.stringify(job.payload),
        job.priority,
        String(readyScore(job.priority, 0)),
        String(priorityBandMs),
        String(maxAttempts),
        job.runAt === undefined ? 'delay' : 'absolute',
        String(job.runAt ?? job.delayMs ?? 0),
        String(policy.baseDelayMs),
        String(policy.maxDelayMs),
        String(policy.jitterMs),
      ],
    );

    if (reply[0] === 'JOB_EXISTS') throw new JobExistsError(id);
    if (reply[0] === 'INVALID_TIME')
      throw new RangeError('Scheduled time exceeds the safe integer range');
    if (reply[0] !== 'ENQUEUED')
      throw new QueueInvariantError(`Enqueue ${id}: ${reply[0]}`);
    const [, status, enqueuedAt, runAt] = enqueueReply.parse(reply);
    this.record({
      jobId: id,
      from: null,
      to: status,
      at: enqueuedAt,
      attempt: 0,
      detail: {
        type: job.type,
        payload: job.payload,
        priority: job.priority,
        maxAttempts,
        runAt,
      },
    });
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
      const reply = await runScript(
        this.redis,
        scripts.promoteDue,
        [this.keys.job(id), this.keys.delayed, this.keys.ready],
        [id],
      );
      const [status] = reply;
      if (status === 'NOT_DUE') break;
      if (status === 'SKIPPED') continue;
      if (status !== 'PROMOTED')
        throw new QueueInvariantError(`Promote ${id}: ${status}`);
      const [, at, attempt] = promoteReply.parse(reply);
      this.record({ jobId: id, from: 'delayed', to: 'ready', at, attempt });
      promoted++;
    }
    return promoted;
  }

  async claim(
    workerId: string,
    visibilityTimeoutMs: number,
    signal?: AbortSignal,
  ) {
    claimInput.parse({ workerId, visibilityTimeoutMs });
    if (signal?.aborted) return null;
    const claimId = randomUUID();
    let [jobId] = await this.redis.zrange(this.keys.ready, 0, 0);

    while (jobId !== undefined && !signal?.aborted) {
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
        at,
      ] = claimReply.parse(reply);
      const payload = jsonPayload.parse(JSON.parse(payloadJson));
      this.record({
        jobId,
        from: 'ready',
        to: 'in_flight',
        at,
        attempt,
        workerId,
      });
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
    const reply = await runScript(
      this.redis,
      scripts.ack,
      [this.keys.job(jobId), this.keys.inFlight],
      [jobId, workerId, claimId, fencingToken],
    );
    const [status] = reply;
    if (status === 'JOB_MISSING') throw new JobNotFoundError(jobId);
    if (status === 'INVALID_CLAIM') throw new InvalidClaimError(jobId);
    if (status !== 'ACKED')
      throw new QueueInvariantError(`Ack ${jobId}: ${status}`);
    const [, at, attempt] = ackReply.parse(reply);
    this.record({
      jobId,
      from: 'in_flight',
      to: 'succeeded',
      at,
      attempt,
      workerId,
    });
  }

  async heartbeat(
    credential: ClaimCredential,
    visibilityTimeoutMs: number,
  ): Promise<number> {
    const { jobId, workerId, claimId, fencingToken } =
      claimCredential.parse(credential);
    claimInput.parse({ workerId, visibilityTimeoutMs });
    const reply = await runScript(
      this.redis,
      scripts.heartbeat,
      [this.keys.job(jobId), this.keys.inFlight],
      [jobId, workerId, claimId, fencingToken, String(visibilityTimeoutMs)],
    );
    if (reply[0] === 'JOB_MISSING') throw new JobNotFoundError(jobId);
    if (reply[0] === 'INVALID_CLAIM') throw new InvalidClaimError(jobId);
    if (reply[0] === 'INVALID_TIME')
      throw new RangeError('Lease deadline exceeds the safe integer range');
    if (reply[0] !== 'EXTENDED')
      throw new QueueInvariantError(`Heartbeat ${jobId}: ${reply[0]}`);
    return heartbeatReply.parse(reply)[1];
  }

  async reapExpired(limit = 100) {
    const batchSize = promoteLimit.parse(limit);
    const [seconds, microseconds] = await this.redis.time();
    const now =
      Number(seconds) * 1000 + Math.floor(Number(microseconds) / 1000);
    const ids = await this.redis.zrangebyscore(
      this.keys.inFlight,
      '-inf',
      now,
      'LIMIT',
      0,
      batchSize,
    );
    let requeued = 0;
    let dead = 0;
    for (const id of ids) {
      const reply = await runScript(
        this.redis,
        scripts.reap,
        [
          this.keys.job(id),
          this.keys.inFlight,
          this.keys.delayed,
          this.keys.dead,
          this.keys.dlqArchive,
        ],
        [id, this.name, this.randomSample(), ...fallbackRetry],
      );
      const [status] = reply;
      if (status === 'NOT_EXPIRED' || status === 'SKIPPED') continue;
      if (status !== 'FAILED')
        throw new QueueInvariantError(`Reap ${id}: ${status}`);
      const result = this.recordFailure(id, reply, 'Visibility lease expired');
      if (result.status === 'delayed') requeued++;
      else dead++;
    }
    return { requeued, dead };
  }

  async fail(credential: ClaimCredential, error: unknown) {
    const { jobId, workerId, claimId, fencingToken } =
      claimCredential.parse(credential);
    const lastError = (
      error instanceof Error
        ? error.message
        : typeof error === 'string'
          ? error
          : 'Handler threw a non-Error value'
    ).replaceAll('\0', '\\0');
    const stack = (
      error instanceof Error ? (error.stack ?? '') : ''
    ).replaceAll('\0', '\\0');
    const reply = await runScript(
      this.redis,
      scripts.fail,
      [
        this.keys.job(jobId),
        this.keys.inFlight,
        this.keys.delayed,
        this.keys.dead,
        this.keys.dlqArchive,
      ],
      [
        jobId,
        this.name,
        workerId,
        claimId,
        fencingToken,
        lastError,
        stack,
        this.randomSample(),
        ...fallbackRetry,
      ],
    );
    if (reply[0] === 'JOB_MISSING') throw new JobNotFoundError(jobId);
    if (reply[0] === 'INVALID_CLAIM') throw new InvalidClaimError(jobId);
    if (reply[0] !== 'FAILED')
      throw new QueueInvariantError(`Fail ${jobId}: ${reply[0]}`);
    return this.recordFailure(jobId, reply, lastError);
  }

  async listDead(offset = 0, limit = 50) {
    timestamp.parse(offset);
    promoteLimit.parse(limit);
    const end = timestamp.parse(offset + limit - 1);
    const ids = await this.redis.zrevrange(this.keys.dead, offset, end);
    const rows = await Promise.all(
      ids.map(async (id) => {
        const hash = await this.redis.hgetall(this.keys.job(id));
        if (hash.status !== 'dead') return null; // Replay may have removed it after the index read.
        const { status, payload, ...row } = dlqEntry.parse(hash);
        return {
          id,
          status,
          ...row,
          payload: jsonPayload.parse(JSON.parse(payload)),
        };
      }),
    );
    return rows.filter((row) => row !== null);
  }

  async depth() {
    // MULTI/EXEC reads all four counters from the same Redis snapshot.
    const counts = await this.redis
      .multi()
      .zcard(this.keys.ready)
      .zcard(this.keys.delayed)
      .zcard(this.keys.inFlight)
      .zcard(this.keys.dead)
      .exec();
    const values = counts?.map(([error, value]) => {
      if (error) throw error;
      return value;
    });
    const parsed = queueDepthReply.safeParse(values);
    if (!parsed.success)
      throw new QueueInvariantError('Invalid queue depth results');
    const [ready, delayed, inFlight, dead] = parsed.data;
    return { ready, delayed, inFlight, dead };
  }

  async listInFlight(offset = 0, limit = 50) {
    timestamp.parse(offset);
    promoteLimit.parse(limit);
    const ids = await this.redis.zrange(
      this.keys.inFlight,
      offset,
      timestamp.parse(offset + limit - 1),
    );
    const rows = await Promise.all(
      ids.map(async (id) => {
        const reply = await runScript(
          this.redis,
          scripts.inspectInFlight,
          [this.keys.job(id), this.keys.inFlight],
          [id],
        );
        if (reply[0] === 'SKIPPED') return null;
        if (reply[0] !== 'IN_FLIGHT')
          throw new QueueInvariantError(`Inspect lease ${id}: ${reply[0]}`);
        const [, type, workerId, attempt, leaseDeadline, leaseRemainingMs] =
          inFlightReply.parse(reply);
        return { id, type, workerId, attempt, leaseDeadline, leaseRemainingMs };
      }),
    );
    return rows.filter((row) => row !== null);
  }

  async purge(selection: ReplaySelection): Promise<void> {
    const { id, replayCount } = replaySelection.parse(selection);
    const reply = await runScript(
      this.redis,
      scripts.purge,
      [this.keys.job(id), this.keys.dead, this.keys.purged],
      [id, String(replayCount)],
    );
    if (reply[0] === 'JOB_MISSING') throw new JobNotFoundError(id);
    if (reply[0] === 'DEAD_CONFLICT') throw new DeadGenerationConflictError(id);
    if (reply[0] !== 'PURGED')
      throw new QueueInvariantError(`Purge ${id}: ${reply[0]}`);
    const [, at, attempt] = purgeReply.parse(reply);
    this.record({
      jobId: id,
      from: 'dead',
      to: 'purged',
      at,
      attempt,
      detail: { replayCount },
    });
  }

  async replay(selection: ReplaySelection) {
    const { id, replayCount } = replaySelection.parse(selection);
    const reply = await runScript(
      this.redis,
      scripts.replay,
      [
        this.keys.job(id),
        this.keys.dead,
        this.keys.ready,
        this.keys.dlqArchive,
      ],
      [id, this.name, String(replayCount)],
    );
    if (reply[0] === 'JOB_MISSING') throw new JobNotFoundError(id);
    if (reply[0] === 'REPLAY_CONFLICT') throw new ReplayConflictError(id);
    if (reply[0] !== 'REPLAYED')
      throw new QueueInvariantError(`Replay ${id}: ${reply[0]}`);
    const [, generation, at, previousAttempt] = replayReply.parse(reply);
    this.record({
      jobId: id,
      from: 'dead',
      to: 'ready',
      at,
      attempt: 0,
      detail: { replayCount: generation, previousAttempt },
    });
    return { id, replayCount: generation };
  }

  async replayMany(selections: readonly ReplaySelection[]) {
    const batch = z.array(replaySelection).max(100).parse(selections);
    const results = [];
    for (const selection of batch) {
      try {
        results.push({
          status: 'replayed' as const,
          ...(await this.replay(selection)),
        });
      } catch (error) {
        results.push({ status: 'failed' as const, id: selection.id, error });
      }
    }
    return results;
  }

  private randomSample() {
    const value = (this.options.random ?? Math.random)();
    if (!(value >= 0 && value < 1))
      throw new RangeError('Retry random sample must be in [0, 1)');
    return String(value);
  }

  private recordFailure(jobId: string, reply: unknown[], lastError: string) {
    const [, status, runAt, at, attempt, workerId] = failureReply.parse(reply);
    this.record({
      jobId,
      from: 'in_flight',
      to: status,
      at,
      attempt,
      workerId,
      detail: { lastError, runAt: status === 'delayed' ? runAt : null },
    });
    return status === 'delayed' ? { status, runAt } : { status };
  }

  private record(event: Omit<LedgerEvent, 'queueName'>) {
    try {
      this.options.record?.({ ...event, queueName: this.name });
    } catch {
      // Redis already committed. An audit sink cannot turn a successful queue operation into a failure.
    }
  }
}

export type ClaimedJob = NonNullable<Awaited<ReturnType<Queue['claim']>>>;
