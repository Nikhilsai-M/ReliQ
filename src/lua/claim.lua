-- KEYS: candidate hash, ready set, in-flight set.
-- ARGV: candidate job ID, worker ID, claim ID, visibility timeout in milliseconds.
local head = redis.call('ZRANGE', KEYS[2], 0, 0)[1] -- Observe the current head atomically with the claim.
if not head then
  return {'EMPTY'} -- The ready set is actually empty.
end -- Finish the empty check.
if head ~= ARGV[1] then
  return {'CONTENDED', head} -- Give the caller the new candidate and let it retry immediately.
end -- Finish the head check before using the explicit candidate hash key.

local job = redis.call('HMGET', KEYS[1], 'status', 'type', 'payload', 'priority', 'attempts', 'maxAttempts', 'fencingToken') -- Load the candidate.
if not job[1] then
  return {'JOB_MISSING'} -- A ready entry without a job is an invariant violation.
end -- Finish the existence check.
local attempts = tonumber(job[5]) -- The counter is bounded by the attempt budget.
local maxAttempts = tonumber(job[6]) -- Maximum number of claims allowed before recovery needs replay.
if job[1] ~= 'ready' or not job[2] or not job[3] or not job[4] or not job[7]
    or not attempts or not maxAttempts or attempts < 0 or attempts >= maxAttempts
    or maxAttempts > 2147483647 or tostring(attempts) ~= job[5] or tostring(maxAttempts) ~= job[6]
    or redis.call('ZSCORE', KEYS[3], ARGV[1]) then
  return {'INCONSISTENT'} -- Reject invalid state or conflicting membership before writing.
end -- Finish the claim preconditions.

local time = redis.call('TIME') -- Set the lease based on Redis time.
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000) -- Convert to milliseconds.
local deadline = now + tonumber(ARGV[4]) -- Resolve the visibility timeout to an absolute deadline.
if deadline > 9007199254740991 then
  return {'INVALID_TIME'} -- An unsafe deadline cannot be represented exactly.
end -- Finish validating the deadline.

redis.call('HINCRBY', KEYS[1], 'fencingToken', 1) -- Increment using Redis's integer arithmetic.
local token = redis.call('HGET', KEYS[1], 'fencingToken') -- Read back the exact decimal string.
local attempt = redis.call('HINCRBY', KEYS[1], 'attempts', 1) -- Count the attempt when ownership starts.
redis.call('HSET', KEYS[1], 'status', 'in_flight', 'workerId', ARGV[2], 'claimId', ARGV[3]) -- Record the active claim.
redis.call('ZADD', KEYS[3], string.format('%.0f', deadline), ARGV[1]) -- Index the lease for expiry scans.
redis.call('ZREM', KEYS[2], ARGV[1]) -- Remove this job from ready before another script runs.
return {'CLAIMED', job[2], job[3], job[4], attempt, maxAttempts, token, deadline, now} -- Return the job, lease details and claim time.
