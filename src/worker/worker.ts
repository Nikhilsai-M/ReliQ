import { setTimeout } from 'node:timers/promises';
import type { Logger } from 'pino';
import type { ClaimedJob, Queue } from '../queue/queue.js';
import { maintainQueue } from '../reaper/maintenance.js';

export type Handler = (job: ClaimedJob, signal: AbortSignal) => Promise<void>;

type WorkerOptions = {
  workerId: string;
  visibilityTimeoutMs: number;
  heartbeatIntervalMs: number;
  pollIntervalMs: number;
  maintenanceIntervalMs: number;
  drainTimeoutMs: number;
};

export class Worker {
  private readonly stop = new AbortController();
  private readonly abandon = new AbortController();
  private running: Promise<void> | undefined;
  private draining: Promise<boolean> | undefined;

  constructor(
    private readonly queue: Queue,
    private readonly handlers: ReadonlyMap<string, Handler>,
    private readonly options: WorkerOptions,
    private readonly logger: Logger,
  ) {}

  run(): Promise<void> {
    this.running ??= this.runLoops();
    return this.running;
  }

  drain(): Promise<boolean> {
    this.stop.abort();
    this.draining ??= this.waitForDrain();
    return this.draining;
  }

  private async waitForDrain(): Promise<boolean> {
    const timer = new AbortController();
    try {
      const drained = await Promise.race([
        this.running?.then(() => true) ?? true,
        setTimeout(this.options.drainTimeoutMs, false, {
          signal: timer.signal,
        }),
      ]);
      if (!drained)
        this.abandon.abort(new Error('Worker drain deadline exceeded'));
      return drained;
    } finally {
      timer.abort();
    }
  }

  private async runLoops() {
    await Promise.all([
      this.consume(),
      maintainQueue(
        this.queue,
        this.options.maintenanceIntervalMs,
        this.stop.signal,
        this.logger,
      ),
    ]);
  }

  private async consume() {
    while (!this.stop.signal.aborted) {
      try {
        const job = await this.queue.claim(
          this.options.workerId,
          this.options.visibilityTimeoutMs,
          this.stop.signal,
        );
        if (job) {
          // A claim submitted before shutdown belongs to the drain, even if its reply arrives during it.
          if (!this.abandon.signal.aborted) await this.handle(job);
          continue;
        }
      } catch (err) {
        this.logger.warn({ err }, 'Claim failed');
      }

      try {
        await setTimeout(this.options.pollIntervalMs, undefined, {
          signal: this.stop.signal,
        });
      } catch (error) {
        if (!this.stop.signal.aborted) throw error;
      }
    }
  }

  private async handle(job: ClaimedJob) {
    const lostLease = new AbortController();
    const signal = AbortSignal.any([this.abandon.signal, lostLease.signal]);
    const stopHeartbeat = new AbortController();
    const heartbeat = this.renewLease(
      job,
      lostLease,
      AbortSignal.any([signal, stopHeartbeat.signal]),
    );
    const { jobId, attempt } = job;

    try {
      const handler = this.handlers.get(job.type);
      if (!handler) throw new Error(`No handler registered for ${job.type}`);
      await handler(job, signal);
      // Finish any renewal before ack removes the lease that heartbeat checks.
      stopHeartbeat.abort();
      await heartbeat;
      if (!signal.aborted) {
        await this.queue.ack(job);
        this.logger.info({ jobId, attempt }, 'Job acknowledged');
      }
    } catch (err) {
      // Phase 4 will add explicit failure reporting. For now the lease expires and the reaper recovers it.
      this.logger.warn(
        { err, jobId, attempt },
        'Job ended without acknowledgment',
      );
    } finally {
      stopHeartbeat.abort();
      await heartbeat;
    }
  }

  private async renewLease(
    job: ClaimedJob,
    lostLease: AbortController,
    signal: AbortSignal,
  ) {
    try {
      while (!signal.aborted) {
        await setTimeout(this.options.heartbeatIntervalMs, undefined, {
          signal,
        });
        if (signal.aborted) return;
        await this.queue.heartbeat(job, this.options.visibilityTimeoutMs);
      }
    } catch (err) {
      if (!signal.aborted) {
        lostLease.abort(err);
        this.logger.warn(
          { err, jobId: job.jobId },
          'Heartbeat failed, handler cancelled',
        );
      }
    }
  }
}
