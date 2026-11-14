export const priorities = ['high', 'normal', 'low'] as const;
export type Priority = (typeof priorities)[number];

export const priorityBandMs = 10_000_000_000_000;

const priorityRank: Record<Priority, number> = {
  high: 0,
  normal: 1,
  low: 2,
};

export function readyScore(priority: Priority, enqueueTimeMs: number): number {
  if (
    !Number.isSafeInteger(enqueueTimeMs) ||
    enqueueTimeMs < 0 ||
    enqueueTimeMs >= priorityBandMs
  ) {
    throw new RangeError('Enqueue timestamp must fit inside one priority band');
  }
  return priorityRank[priority] * priorityBandMs + enqueueTimeMs;
}
