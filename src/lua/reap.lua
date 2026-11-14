-- KEYS: job hash, in-flight set, ready set, dead set, archive stream. ARGV: job ID, queue name.
local deadline = tonumber(redis.call('ZSCORE', KEYS[2], ARGV[1])) -- Recheck the lease after the candidate scan.
if not deadline then
  return {'SKIPPED'} -- It already transitioned or was never in flight.
end -- Finish the current membership check.
local time = redis.call('TIME') -- Expiry is decided inside Redis.
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000) -- Convert to milliseconds.
if deadline > now then
  return {'NOT_EXPIRED'} -- A valid or recently renewed lease stays in flight.
end -- Finish comparing the current deadline.

local job = redis.call('HMGET', KEYS[1], 'status', 'attempts', 'maxAttempts', 'readyScore', 'type', 'payload', 'replayCount') -- Load the transition inputs.
if not job[1] then
  return {'JOB_MISSING'} -- Keep the orphaned entry visible for repair.
end -- Finish the existence check.
local attempts = tonumber(job[2]) -- Attempts already include the expired claim.
local maxAttempts = tonumber(job[3]) -- Reaching the limit prevents another claim.
if job[1] ~= 'in_flight' or not attempts or not maxAttempts or attempts < 1 or not tonumber(job[4])
    or redis.call('ZSCORE', KEYS[3], ARGV[1]) or redis.call('ZSCORE', KEYS[4], ARGV[1]) then
  return {'INCONSISTENT'} -- Validate status and target indexes before mutating anything.
end -- Finish checking the job and indexes.
local lastError = 'Visibility lease expired' -- Recovery has no handler stack to capture.
local nowText = string.format('%.0f', now) -- Preserve the full timestamp in command arguments.

if attempts >= maxAttempts then
  if not job[5] or not job[6] or not job[7] then
    return {'INCONSISTENT'} -- A terminal job must have enough data for the archive.
  end -- Finish validating the archive snapshot.
  redis.call('XADD', KEYS[5], '*', 'event', 'dead', 'jobId', ARGV[1], 'queueName', ARGV[2],
    'type', job[5], 'payload', job[6], 'replayCount', job[7], 'attempts', job[2], -- Capture the terminal generation.
    'lastError', lastError, 'deadAt', nowText) -- Store archive work alongside the terminal transition.
  redis.call('ZADD', KEYS[4], nowText, ARGV[1]) -- Index the terminal job for later DLQ operations.
  redis.call('HSET', KEYS[1], 'status', 'dead', 'deadAt', nowText, 'lastError', lastError) -- Record why it stopped.
else -- With attempts left, Phase 3 makes the job ready immediately.
  redis.call('ZADD', KEYS[3], job[4], ARGV[1]) -- Reuse the original priority and enqueue ordering.
  redis.call('HSET', KEYS[1], 'status', 'ready', 'runAt', nowText, 'lastError', lastError) -- Make the job eligible now.
end -- Finish recording the destination.
redis.call('ZREM', KEYS[2], ARGV[1]) -- Clear the expired lease without touching the fencing counter.
redis.call('HDEL', KEYS[1], 'workerId', 'claimId', 'stack') -- Remove ownership and any previous stack.
if attempts >= maxAttempts then
  return {'DEAD'} -- The next claim requires operator replay in a later phase.
end -- Finish choosing the terminal result.
return {'REQUEUED'} -- The next worker will increment the existing fencing counter.
