-- KEYS: job hash, ready set, delayed set.
-- ARGV: job ID, type, JSON payload, priority, band offset, band width, attempt limit, schedule mode, schedule value, base delay, max delay, jitter.
-- The same ID cannot create an additional job, even if the original was already acknowledged.
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {'JOB_EXISTS'} -- Leave the original job untouched.
end -- Finish duplicate detection.

-- Redis resolves relative schedules against its own wall clock.
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000) -- Seconds and microseconds to milliseconds.
local band = tonumber(ARGV[6]) -- One band covers every supported enqueue timestamp.
if now < 0 or now >= band then
  return {'INVALID_CLOCK'} -- Stop before two priority bands could overlap.
end -- Finish checking the score input.
local runAt = tonumber(ARGV[9]) -- Absolute timestamps are already in milliseconds.
if ARGV[8] == 'delay' then
  runAt = now + runAt -- Convert relative delay to a timestamp.
end -- Finish schedule resolution.
if runAt > 9007199254740991 then
  return {'INVALID_TIME'} -- The computed timestamp must fit within JavaScript's safe integer range.
end -- Finish checking timestamp precision.

-- Validate both indexes before writing and reject pre-existing entries without a hash.
if redis.call('ZSCORE', KEYS[2], ARGV[1]) or redis.call('ZSCORE', KEYS[3], ARGV[1]) then
  return {'INCONSISTENT'} -- An orphaned entry needs operator attention.
end -- Finish index validation.
local status = 'ready' -- Overdue jobs enter ready directly.
local destination = KEYS[2] -- Select the ready index by default.
local readyScore = string.format('%.0f', tonumber(ARGV[5]) + now) -- Add Redis time to the priority's band offset.
local score = readyScore -- An immediate job uses the packed score now.
local runAtText = string.format('%.0f', runAt) -- Keep the full integer in Redis command arguments.
if runAt > now then
  status = 'delayed' -- Future jobs stay invisible to claim until promoted.
  destination = KEYS[3] -- Select the delayed index.
  score = runAtText -- Order delayed jobs by their eligibility time.
end -- Finish selecting the state and index.

-- Initialize the record before adding it to the selected index within this same script.
redis.call('HSET', KEYS[1],
  'type', ARGV[2], 'payload', ARGV[3], 'priority', ARGV[4], -- Immutable job contents.
  'enqueuedAt', string.format('%.0f', now), 'runAt', runAtText, 'readyScore', readyScore, -- Scheduling metadata.
  'baseDelayMs', ARGV[10], 'maxDelayMs', ARGV[11], 'jitterMs', ARGV[12], -- Snapshot the configured retry policy.
  'status', status, 'attempts', '0', 'maxAttempts', ARGV[7], -- Initial execution state.
  'fencingToken', '0', 'replayCount', '0') -- Counters start at zero.
redis.call('ZADD', destination, score, ARGV[1]) -- Index the new job under its ID.
return {'ENQUEUED', status, now, runAt} -- Return resolved scheduling metadata to the caller.
