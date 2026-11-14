type Status = 'ready' | 'delayed' | 'in_flight' | 'succeeded' | 'dead';

export type LedgerEvent = {
  jobId: string;
  queueName: string;
  from: Status | null;
  to: Status;
  at: number;
  attempt: number;
  workerId?: string;
  detail?: object;
};
