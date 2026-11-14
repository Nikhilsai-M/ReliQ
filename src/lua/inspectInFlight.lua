-- KEYS: job hash, in-flight set. ARGV: job ID.
local deadline = tonumber(redis.call('ZSCORE', KEYS[2], ARGV[1])) -- Read the current lease after the page read.
if not deadline then
  return {'SKIPPED'} -- The candidate already left in-flight.
end -- Finish checking membership.
local job = redis.call('HMGET', KEYS[1], 'status', 'type', 'workerId', 'attempts') -- Read the owner and attempt from the same atomic view.
if job[1] ~= 'in_flight' or not job[2] or not job[3] or not tonumber(job[4]) then
  return {'INCONSISTENT'} -- A live lease requires a matching job and worker.
end -- Finish checking the corresponding hash.
local time = redis.call('TIME') -- Measure remaining time using the same clock as the lease.
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000) -- Convert to milliseconds.
return {'IN_FLIGHT', job[2], job[3], tonumber(job[4]), deadline, math.max(0, deadline - now)} -- Clamp expired leases to zero remaining.
