import { describe, expect, it } from 'vitest';
import { priorityBandMs, readyScore } from '../../src/queue/priority.js';

describe('ready score', () => {
  it('puts every higher-priority score before every lower-priority score', () => {
    expect(readyScore('high', priorityBandMs - 1)).toBeLessThan(
      readyScore('normal', 0),
    );
    expect(readyScore('normal', priorityBandMs - 1)).toBeLessThan(
      readyScore('low', 0),
    );
  });

  it('preserves single-millisecond order and precision within the largest band', () => {
    const last = readyScore('low', priorityBandMs - 1);
    expect(Number.isSafeInteger(last)).toBe(true);
    expect(last).toBe(29_999_999_999_999);
    expect(last - readyScore('low', priorityBandMs - 2)).toBe(1);
  });

  it.each([-1, 0.5, priorityBandMs, Infinity])(
    'rejects out-of-band timestamp %s',
    (time) => {
      expect(() => readyScore('high', time)).toThrow(RangeError);
    },
  );
});
