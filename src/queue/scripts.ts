import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import type { Redis } from 'ioredis';
import { QueueInvariantError } from './errors.js';

type Script = { source: string; sha: string };

function loadScript(name: string): Script {
  const source = readFileSync(
    new URL(`../lua/${name}.lua`, import.meta.url),
    'utf8',
  );
  return { source, sha: createHash('sha1').update(source).digest('hex') };
}

export const scripts = {
  enqueue: loadScript('enqueue'),
  promoteDue: loadScript('promoteDue'),
  claim: loadScript('claim'),
  ack: loadScript('ack'),
};

export async function runScript(
  redis: Redis,
  script: Script,
  keys: string[],
  args: string[],
): Promise<[status: string, ...data: unknown[]]> {
  let result: unknown;
  try {
    result = await redis.evalsha(script.sha, keys.length, ...keys, ...args);
  } catch (error) {
    // NOSCRIPT means execution never started, so it is safe to send the script body.
    if (!(error instanceof Error) || !error.message.startsWith('NOSCRIPT '))
      throw error;
    result = await redis.eval(script.source, keys.length, ...keys, ...args);
  }

  if (!Array.isArray(result) || typeof result[0] !== 'string') {
    throw new QueueInvariantError('Unexpected Redis script response');
  }
  return [result[0], ...result.slice(1)];
}
