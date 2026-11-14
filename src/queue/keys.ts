import { z } from 'zod';

const queueName = z.string().regex(/^[A-Za-z0-9_-]+$/);

export function queueKeys(name: string) {
  const prefix = `reliq:{${queueName.parse(name)}}`;
  return {
    ready: `${prefix}:ready`,
    delayed: `${prefix}:delayed`,
    inFlight: `${prefix}:in_flight`,
    job: (id: string) => `${prefix}:job:${id}`,
  };
}
