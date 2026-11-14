-- KEYS: job hash, delayed set, ready set. ARGV: job ID.
local delayedScore = redis.call('ZSCORE', KEYS[2], ARGV[1]) -- Read the current schedule, which may differ from the scan.
if not delayedScore then
  return {'SKIPPED'} -- Another promoter already moved it, or it was removed.
end -- Finish the membership check.

local time = redis.call('TIME') -- The caller's clock cannot make the job eligible.
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000) -- Convert to milliseconds.
if tonumber(delayedScore) > now then
  return {'NOT_DUE'} -- Leave the job in delayed until its deadline.
end -- Finish the eligibility check.

local job = redis.call('HMGET', KEYS[1], 'status', 'readyScore') -- Read state and the original score together.
if not job[1] then
  return {'JOB_MISSING'} -- The delayed index points to a missing job or status.
end -- Finish the existence check.
if job[1] ~= 'delayed' or not tonumber(job[2]) or redis.call('ZSCORE', KEYS[3], ARGV[1]) then
  return {'INCONSISTENT'} -- Validate state and the destination before any mutation.
end -- Finish checking the transition preconditions.

redis.call('ZADD', KEYS[3], job[2], ARGV[1]) -- Restore the original priority and enqueue-time ordering.
redis.call('ZREM', KEYS[2], ARGV[1]) -- Remove delayed membership in the same transition.
redis.call('HSET', KEYS[1], 'status', 'ready') -- Keep the hash consistent with the indexes.
return {'PROMOTED'} -- Count this candidate once, even when promoters race.
