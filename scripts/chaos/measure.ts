import type { Pool } from 'pg';

export async function measure(
  pool: Pool,
  acknowledged: string[],
  source: string,
  destination: string,
) {
  const {
    rows: [metrics],
  } = await pool.query<{
    handler_executions: number;
    jobs_entered: number;
    side_effects: number;
    duplicated_effects: number;
    debits: number;
    credits: number;
    receipt_effects: number;
  }>(
    `WITH effects AS (
    SELECT job_id, step_key, count(*)::int AS applications FROM transfer_effects
    WHERE job_id = ANY($1::uuid[]) GROUP BY job_id, step_key
  ) SELECT
    (SELECT count(*)::int FROM transfer_executions WHERE job_id = ANY($1::uuid[])) AS handler_executions,
    (SELECT count(DISTINCT job_id)::int FROM transfer_executions WHERE job_id = ANY($1::uuid[])) AS jobs_entered,
    COALESCE(sum(applications), 0)::int AS side_effects,
    COALESCE(sum(applications - 1), 0)::int AS duplicated_effects,
    COALESCE(sum(applications) FILTER (WHERE step_key = 'debit'), 0)::int AS debits,
    COALESCE(sum(applications) FILTER (WHERE step_key = 'credit'), 0)::int AS credits,
    COALESCE(sum(applications) FILTER (WHERE step_key = 'receipt'), 0)::int AS receipt_effects
    FROM effects`,
    [acknowledged],
  );
  if (!metrics) throw new Error('Missing effect totals');
  const { rows: receipts } = await pool.query<{ job_id: string }>(
    'SELECT job_id FROM receipts WHERE job_id = ANY($1::uuid[])',
    [acknowledged],
  );
  const { rows: balances } = await pool.query<{
    id: string;
    balance_cents: string;
  }>('SELECT id, balance_cents FROM accounts WHERE id = ANY($1::uuid[])', [
    [source, destination],
  ]);
  return {
    ...metrics,
    receipts: new Set(receipts.map(({ job_id }) => job_id)),
    balances: Object.fromEntries(
      balances.map(({ id, balance_cents }) => [id, Number(balance_cents)]),
    ),
  };
}
