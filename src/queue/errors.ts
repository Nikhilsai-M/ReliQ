export class JobExistsError extends Error {
  constructor(readonly jobId: string) {
    super(`Job already exists: ${jobId}`);
    this.name = 'JobExistsError';
  }
}

export class JobNotFoundError extends Error {
  constructor(readonly jobId: string) {
    super(`Job not found: ${jobId}`);
    this.name = 'JobNotFoundError';
  }
}

export class InvalidClaimError extends Error {
  constructor(readonly jobId: string) {
    super(`Claim is stale, expired, or no longer active: ${jobId}`);
    this.name = 'InvalidClaimError';
  }
}

export class QueueInvariantError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'QueueInvariantError';
  }
}

export class ReplayConflictError extends Error {
  constructor(readonly jobId: string) {
    super(
      `Job no longer matches the selected dead-letter generation: ${jobId}`,
    );
    this.name = 'ReplayConflictError';
  }
}
