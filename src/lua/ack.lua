-- KEYS: job hash, in-flight set. ARGV: job ID, worker ID, claim ID, fencing token.
local job = redis.call('HMGET', KEYS[1], 'status', 'workerId', 'claimId', 'fencingToken', 'attempts') -- Read the active credential and attempt.
if not job[1] then
  return {'JOB_MISSING'} -- Missing jobs have no owner to acknowledge them.
end -- Finish the existence check.
if job[1] ~= 'in_flight' or job[2] ~= ARGV[2] or job[3] ~= ARGV[3] or job[4] ~= ARGV[4] then
  return {'INVALID_CLAIM'} -- Reject the wrong owner, stale token, or a repeated acknowledgment.
end -- Finish checking the credential using exact string comparisons.

local deadline = redis.call('ZSCORE', KEYS[2], ARGV[1]) -- Read the lease deadline from the index.
if not deadline or not tonumber(job[5]) then
  return {'INCONSISTENT'} -- An in-flight job must have a lease.
end -- Finish the lease membership check.
local time = redis.call('TIME') -- Compare the deadline with Redis time.
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000) -- Convert to milliseconds.
if tonumber(deadline) <= now then
  return {'INVALID_CLAIM'} -- Expiry invalidates ownership even before the reaper runs.
end -- Finish guarding against an expired owner.

redis.call('ZREM', KEYS[2], ARGV[1]) -- Remove the live lease.
redis.call('HSET', KEYS[1], 'status', 'succeeded') -- Retain the terminal job for inspection.
redis.call('HDEL', KEYS[1], 'workerId', 'claimId') -- Clear ownership while retaining the fencing counter.
return {'ACKED', now, tonumber(job[5])} -- Return the transition time and attempt for the ledger.
