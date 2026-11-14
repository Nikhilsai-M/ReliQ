import { z } from 'zod';

const claim = { jobId: z.uuid(), claimId: z.uuid(), atMs: z.number() };
export const workerMessage = z.discriminatedUnion('event', [
  z.object({ event: z.literal('ready') }),
  z.object({ event: z.literal('debit_committed'), jobId: z.uuid() }),
  z.object({ event: z.literal('handler_started'), ...claim }),
  z.object({ event: z.literal('completed'), ...claim }),
  z.object({ event: z.literal('ack_failed'), ...claim }),
]);

type Trace = Extract<z.infer<typeof workerMessage>, { claimId: string }> & {
  workerId: string;
};

export function classifyRepeats(
  trace: Trace[],
  paused: { jobId: string; workerId: string },
  outage: { start: number; end: number },
  total: number,
) {
  const starts = trace
    .filter(({ event }) => event === 'handler_started')
    .sort((a, b) => a.atMs - b.atMs);
  const ackFailed = new Set(
    trace
      .filter(
        ({ event, atMs }) =>
          event === 'ack_failed' && atMs >= outage.start && atMs <= outage.end,
      )
      .map(({ claimId }) => claimId),
  );
  const priorByJob = new Map<string, Trace>();
  const counts = {
    killedWorker: 0,
    failedAckDuringRedisRestart: 0,
    other: 0,
    unclassified: 0,
  };
  const repeats = [];
  for (const current of starts) {
    const prior = priorByJob.get(current.jobId);
    priorByJob.set(current.jobId, current);
    if (!prior) continue;
    const cause =
      prior.jobId === paused.jobId && prior.workerId === paused.workerId
        ? 'killedWorker'
        : ackFailed.has(prior.claimId)
          ? 'failedAckDuringRedisRestart'
          : 'other';
    counts[cause]++;
    repeats.push({
      jobId: current.jobId,
      priorClaimId: prior.claimId,
      nextClaimId: current.claimId,
      cause,
    });
  }
  counts.unclassified = total - repeats.length;
  if (counts.unclassified < 0)
    throw new Error('More handler repeats traced than committed starts');
  return { counts, repeats };
}
