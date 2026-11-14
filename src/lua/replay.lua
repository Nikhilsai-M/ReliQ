-- KEYS: job hash, dead set, ready set, archive stream. ARGV: ID, queue, expected replay count.
local job = redis.call('HMGET', KEYS[1], 'status', 'replayCount', 'readyScore', 'attempts') -- Load the current dead generation.
if not job[1] then
  return {'JOB_MISSING'} -- Only retained jobs can be replayed.
end -- Finish verifying existence.
if job[1] ~= 'dead' or job[2] ~= ARGV[3] then
  return {'REPLAY_CONFLICT'} -- A stale request cannot replay a different generation.
end -- Finish checking the state and generation.
if not redis.call('ZSCORE', KEYS[2], ARGV[1]) or not tonumber(job[3]) or not tonumber(job[4])
    or tonumber(job[2]) >= 2147483647 or redis.call('ZSCORE', KEYS[3], ARGV[1]) then
  return {'INCONSISTENT'} -- Validate indexes and counters before writing the archive event.
end -- Finish checking transition preconditions.
local time = redis.call('TIME') -- Timestamp the archive event and new schedule on Redis time.
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000) -- Convert to milliseconds.
local nowText = string.format('%.0f', now) -- Retain the exact timestamp in Redis arguments.

redis.call('XADD', KEYS[4], '*', 'event', 'replay', 'jobId', ARGV[1], 'queueName', ARGV[2],
  'replayCount', job[2], 'replayedAt', nowText) -- Mark the prior generation replayed in PostgreSQL asynchronously.
local replayCount = redis.call('HINCRBY', KEYS[1], 'replayCount', 1) -- Start a distinct recovery generation.
redis.call('HSET', KEYS[1], 'status', 'ready', 'attempts', '0', 'runAt', nowText) -- Reset the budget but preserve the token.
redis.call('HDEL', KEYS[1], 'lastError', 'stack', 'deadAt') -- Clear the previous failure from live state.
redis.call('ZADD', KEYS[3], job[3], ARGV[1]) -- Reuse the original ready score.
redis.call('ZREM', KEYS[2], ARGV[1]) -- Remove the old dead membership atomically.
return {'REPLAYED', replayCount, now, tonumber(job[4])} -- Return the new generation and transition time.
