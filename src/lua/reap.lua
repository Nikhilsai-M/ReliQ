-- KEYS: job hash, in-flight set, delayed set, dead set, archive stream.
-- ARGV: ID, queue, unit random sample, fallback retry policy.
local deadline = tonumber(redis.call('ZSCORE', KEYS[2], ARGV[1])) -- Recheck after the candidate scan.
if not deadline then
  return {'SKIPPED'} -- The candidate already transitioned or was never in flight.
end -- Finish the membership check.
local time = redis.call('TIME') -- Decide expiry using the same clock as heartbeat.
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000) -- Convert to milliseconds.
if deadline > now then
  return {'NOT_EXPIRED'} -- A valid or renewed lease stays in flight.
end -- Finish checking the current deadline.

local status = redis.call('HGET', KEYS[1], 'status') -- Check the hash's current state.
if not status then
  return {'JOB_MISSING'} -- Keep an orphaned index entry visible for repair.
end -- Finish the existence check.
if status ~= 'in_flight' then
  return {'INCONSISTENT'} -- A different state cannot have an active lease.
end -- Finish the state check.
return retryOrDead(KEYS, ARGV[1], ARGV[2], now, tonumber(ARGV[3]), 'Visibility lease expired', '', {ARGV[4], ARGV[5], ARGV[6]}) -- Apply the same backoff as fail.
