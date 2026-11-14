import { randomUUID } from 'node:crypto';
import type { Pool } from 'pg';
import type { Logger } from 'pino';
import { setTimeout } from 'node:timers/promises';
import type { LedgerEvent } from '../queue/ledger-event.js';

type BufferedEvent = Omit<LedgerEvent, 'detail'> & {
  eventId: string;
  detail: string;
};

export class Ledger {
  private readonly pending: BufferedEvent[] = [];
  private dropped = 0;
  private nextDropLog = 0;

  constructor(
    private readonly pool: Pool,
    private readonly logger: Logger,
    private readonly capacity = 1_000,
  ) {}

  record(event: LedgerEvent): void {
    if (this.pending.length >= this.capacity) {
      this.dropped++;
      const now = Date.now();
      if (now >= this.nextDropLog) {
        this.nextDropLog = now + 10_000;
        this.logger.warn({ dropped: this.dropped }, 'Ledger buffer full');
      }
      return;
    }

    const { detail, ...fields } = event;
    try {
      this.pending.push({
        ...fields,
        eventId: randomUUID(),
        detail: JSON.stringify(detail ?? {}),
      });
    } catch (err) {
      this.logger.warn(
        { err, jobId: event.jobId },
        'Ledger event could not be serialized',
      );
    }
  }

  async run(signal: AbortSignal): Promise<void> {
    let failures = 0;
    while (!signal.aborted) {
      const event = this.pending[0];
      let delay = 100;
      if (event) {
        try {
          await this.pool.query(
            `
            INSERT INTO jobs_ledger (event_id, job_id, queue_name, "from", "to", at, worker_id, attempt, detail)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::json)
            ON CONFLICT (event_id) DO NOTHING`,
            [
              event.eventId,
              event.jobId,
              event.queueName,
              event.from,
              event.to,
              new Date(event.at),
              event.workerId?.replaceAll('\0', '\\0') ?? null,
              event.attempt,
              event.detail,
            ],
          );
          this.pending.shift();
          failures = 0;
          continue;
        } catch (err) {
          failures = Math.min(failures + 1, 6);
          delay = Math.min(
            100 * 2 ** failures + Math.floor(Math.random() * 100),
            5_000,
          );
          this.logger.warn(
            { err, delay },
            'Ledger write failed; retry scheduled',
          );
        }
      }

      try {
        await setTimeout(delay, undefined, { signal });
      } catch (err) {
        if (!signal.aborted) throw err;
      }
    }
  }
}
