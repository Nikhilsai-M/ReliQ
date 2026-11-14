import type { Pool, PoolClient } from 'pg';
import { z } from 'zod';
import type { ClaimedJob } from '../queue/queue.js';
import type { Handler } from '../worker/worker.js';

const transferPayload = z
  .object({
    debitAccountId: z.uuid(),
    creditAccountId: z.uuid(),
    amountCents: z.int().positive(),
  })
  .strict()
  .refine(
    ({ debitAccountId, creditAccountId }) => debitAccountId !== creditAccountId,
  );

type Step = 'debit' | 'credit' | 'receipt';
type Effect = (client: PoolClient) => Promise<boolean | void>;

async function applyStep(
  pool: Pool,
  jobId: string,
  step: Step,
  effect: Effect,
) {
  const client = await pool.connect();
  let discard = false;
  try {
    await client.query('BEGIN');
    const inserted = await client.query(
      `INSERT INTO idempotency_keys (job_id, step_key)
      VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING step_key`,
      [jobId, step],
    );
    if (inserted.rowCount === 1) {
      const applied = await effect(client);
      if (applied !== false)
        await client.query(
          'INSERT INTO transfer_effects (job_id, step_key) VALUES ($1, $2)',
          [jobId, step],
        );
    }
    await client.query('COMMIT');
  } catch (error) {
    try {
      await client.query('ROLLBACK');
    } catch {
      discard = true;
    }
    throw error;
  } finally {
    client.release(discard);
  }
}

export function createTransferHandler(
  pool: Pool,
  afterDebit?: (job: ClaimedJob, signal: AbortSignal) => Promise<void>,
  onExecutionRecorded?: (job: ClaimedJob) => void,
): Handler {
  return async (job, signal) => {
    const { jobId, workerId } = job;
    signal.throwIfAborted();
    await pool.query(
      'INSERT INTO transfer_executions (job_id, worker_id) VALUES ($1, $2)',
      [jobId, workerId],
    );
    onExecutionRecorded?.(job);
    const { debitAccountId, creditAccountId, amountCents } =
      transferPayload.parse(job.payload);
    signal.throwIfAborted();
    await applyStep(pool, jobId, 'debit', async (client) => {
      await client.query(
        `INSERT INTO transfers (job_id, debit_account_id, credit_account_id, amount_cents)
        VALUES ($1, $2, $3, $4) ON CONFLICT (job_id) DO NOTHING`,
        [jobId, debitAccountId, creditAccountId, amountCents],
      );
      const debit = await client.query(
        `UPDATE accounts AS account SET balance_cents = account.balance_cents - transfer.amount_cents
        FROM transfers AS transfer WHERE transfer.job_id = $1 AND account.id = transfer.debit_account_id
        AND account.balance_cents >= transfer.amount_cents`,
        [jobId],
      );
      if (debit.rowCount !== 1) throw new Error('Insufficient funds');
    });

    await afterDebit?.(job, signal);
    signal.throwIfAborted();
    await applyStep(pool, jobId, 'credit', async (client) => {
      // Use the committed transfer so subsequent steps use the original account and amount.
      const credit = await client.query(
        `UPDATE accounts AS account
        SET balance_cents = account.balance_cents + transfer.amount_cents
        FROM transfers AS transfer WHERE transfer.job_id = $1 AND account.id = transfer.credit_account_id`,
        [jobId],
      );
      if (credit.rowCount !== 1) throw new Error('Missing transfer to credit');
    });

    signal.throwIfAborted();
    await applyStep(pool, jobId, 'receipt', async (client) => {
      const receipt = await client.query(
        'INSERT INTO receipts (job_id) VALUES ($1) ON CONFLICT (job_id) DO NOTHING',
        [jobId],
      );
      return receipt.rowCount === 1;
    });
  };
}
