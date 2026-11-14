import type { Redis } from 'ioredis';
import type { Pool } from 'pg';

export function createHealthCheck(redis: Redis, pool: Pool) {
  return async () => {
    const [cache, database] = await Promise.allSettled([
      redis.ping(),
      pool.query('SELECT 1'),
    ]);
    const redisUp = cache.status === 'fulfilled' && cache.value === 'PONG';
    const postgresUp = database.status === 'fulfilled';
    return {
      redis: redisUp ? 'up' : 'down',
      postgres: postgresUp ? 'up' : 'down',
    } as const;
  };
}
