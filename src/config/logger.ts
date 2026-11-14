import { pino } from 'pino';
import type { Config } from './env.js';

export function createLogger(level: Config['LOG_LEVEL']) {
  return pino({ name: 'reliq', level });
}
