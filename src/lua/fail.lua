-- KEYS: job hash, in-flight set, delayed set, dead set, archive stream.
-- ARGV: ID, queue, worker ID, claim ID, fencing token, error, stack, unit random sample, fallback retry policy.
local job = redis.call('HMGET', KEYS[1], 'status', 'workerId', 'claimId', 'fencingToken') -- Read the current owner.
if not job[1] then
  return {'JOB_MISSING'} -- A missing job cannot be failed by a worker.
end -- Finish the existence check.
if job[1] ~= 'in_flight' or job[2] ~= ARGV[3] or job[3] ~= ARGV[4] or job[4] ~= ARGV[5] then
  return {'INVALID_CLAIM'} -- Reject stale credentials and repeated calls.
end -- Finish checking the full claim.

local deadline = tonumber(redis.call('ZSCORE', KEYS[2], ARGV[1])) -- Find the authoritative lease deadline.
if not deadline then
  return {'INCONSISTENT'} -- In-flight status requires a lease.
end -- Finish checking lease membership.
local time = redis.call('TIME') -- Use Redis time to check ownership and schedule the retry.
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000) -- Convert to milliseconds.
if deadline <= now then
  return {'INVALID_CLAIM'} -- The old owner cannot fail the job once its lease expires.
end -- Finish the expiry check.
return retryOrDead(KEYS, ARGV[1], ARGV[2], now, tonumber(ARGV[8]), ARGV[6], ARGV[7], {ARGV[9], ARGV[10], ARGV[11]}) -- Apply the retry budget atomically.
