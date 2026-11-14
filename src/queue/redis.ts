import { Redis } from 'ioredis';
import type { Logger } from 'pino';

export function createRedis(url: string, logger: Logger): Redis {
  const redis = new Redis(url, {
    lazyConnect: true,
    connectTimeout: 3_000,
    commandTimeout: 5_000,
    enableOfflineQueue: false,
    autoResendUnfulfilledCommands: false,
    maxRetriesPerRequest: 0,
    retryStrategy: (attempt) => Math.min(attempt * 100, 2_000),
  });

  redis.on('error', (err: Error) =>
    logger.warn({ err }, 'Redis connection error'),
  );
  return redis;
}
