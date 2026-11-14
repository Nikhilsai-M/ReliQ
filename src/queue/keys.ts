import { queueName } from './schemas.js';

export function queueKeys(name: string) {
  const prefix = `reliq:{${queueName.parse(name)}}`;
  return {
    ready: `${prefix}:ready`,
    delayed: `${prefix}:delayed`,
    inFlight: `${prefix}:in_flight`,
    dead: `${prefix}:dead`,
    purged: `${prefix}:purged`,
    dlqArchive: `${prefix}:dlq_archive`,
    job: (id: string) => `${prefix}:job:${id}`,
  };
}
