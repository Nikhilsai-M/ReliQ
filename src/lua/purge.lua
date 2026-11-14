-- KEYS: job hash, dead set, purged ID set. ARGV: job ID, expected replay count.
local job = redis.call('HMGET', KEYS[1], 'status', 'replayCount', 'attempts') -- Read the selected generation.
if not job[1] then
  return {'JOB_MISSING'} -- A purged job has no live record.
end -- Finish the existence check.
if job[1] ~= 'dead' or job[2] ~= ARGV[2] then
  return {'DEAD_CONFLICT'} -- A replay or newer dead generation wins over this stale request.
end -- Finish verifying the state and generation.
if not tonumber(job[3]) or not redis.call('ZSCORE', KEYS[2], ARGV[1]) then
  return {'INCONSISTENT'} -- Verify both records before removing them.
end -- Finish verifying the index.

local time = redis.call('TIME') -- Use the queue clock for the audit event.
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000) -- Convert to milliseconds.
redis.call('SADD', KEYS[3], ARGV[1]) -- Reserve the ID so future enqueues cannot collide with archived generations.
redis.call('ZREM', KEYS[2], ARGV[1]) -- Remove the job from the live dead index.
redis.call('DEL', KEYS[1]) -- Remove the dead hash and leave pending archive events intact.
return {'PURGED', now, tonumber(job[3])} -- Return the audit timestamp and attempt count.
