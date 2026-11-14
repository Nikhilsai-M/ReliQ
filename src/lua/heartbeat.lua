-- KEYS: job hash, in-flight set. ARGV: job ID, worker ID, claim ID, fencing token, visibility timeout.
local job = redis.call('HMGET', KEYS[1], 'status', 'workerId', 'claimId', 'fencingToken') -- Read the current owner.
if not job[1] then
  return {'JOB_MISSING'} -- Missing jobs cannot hold leases.
end -- Finish the existence check.
if job[1] ~= 'in_flight' or job[2] ~= ARGV[2] or job[3] ~= ARGV[3] or job[4] ~= ARGV[4] then
  return {'INVALID_CLAIM'} -- A different or finished claim cannot renew the lease.
end -- Finish comparing the full credential as strings.

local deadline = tonumber(redis.call('ZSCORE', KEYS[2], ARGV[1])) -- Fetch the current deadline.
if not deadline then
  return {'INCONSISTENT'} -- In-flight status requires an index entry.
end -- Finish the membership check.
local time = redis.call('TIME') -- Read the clock used for this lease.
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000) -- Convert to milliseconds.
if deadline <= now then
  return {'INVALID_CLAIM'} -- A heartbeat cannot revive an expired lease.
end -- Finish checking that the owner is still active.
local nextDeadline = math.max(deadline, now + tonumber(ARGV[5])) -- Never shorten the current lease.
if nextDeadline > 9007199254740991 then
  return {'INVALID_TIME'} -- Reject a deadline beyond the safe integer range.
end -- Finish validating the proposed deadline.

redis.call('ZADD', KEYS[2], string.format('%.0f', nextDeadline), ARGV[1]) -- Renew the lease atomically.
return {'EXTENDED', nextDeadline} -- Return the actual deadline, including on a repeat call.
