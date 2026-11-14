-- Calculate delay from the saved policy and sample, without reading or writing queue state.
local function retryDelay(attempt, baseDelay, maxDelay, maxJitter, unitRandom)
  local jitter = math.floor(unitRandom * (maxJitter + 1)) -- Pick an integer from zero through the configured maximum.
  return math.min(baseDelay * 2 ^ math.min(attempt, 53) + jitter, maxDelay) -- Larger exponents already exceed the cap.
end -- Finish calculating the delay.

-- Shared by fail and reap. Each caller validates its own ownership or expiry before calling this function.
local function retryOrDead(keys, id, queue, now, unitRandom, lastError, stack, fallback)
  -- KEYS: job hash, in-flight set, delayed set, dead set, archive stream.
  local job = redis.call('HMGET', keys[1], 'attempts', 'maxAttempts', 'type', 'payload', 'replayCount', 'baseDelayMs', 'maxDelayMs', 'jitterMs', 'workerId')
  local attempts = tonumber(job[1]) -- The attempt was counted at claim time.
  local maxAttempts = tonumber(job[2]) -- Both failure paths consume the same budget.
  local baseDelay = tonumber(job[6] or fallback[1]) -- Older jobs use the original default policy.
  local maxDelay = tonumber(job[7] or fallback[2]) -- Bound retry latency.
  local maxJitter = tonumber(job[8] or fallback[3]) -- Spread jobs whose retries would otherwise coincide.
  if not attempts or attempts < 1 or not maxAttempts or not job[3] or not job[4] or not job[5] or not job[9]
      or not baseDelay or baseDelay < 1 or not maxDelay or maxDelay < baseDelay or not maxJitter or maxJitter < 0
      or not unitRandom or unitRandom < 0 or unitRandom >= 1
      or redis.call('ZSCORE', keys[3], id) or redis.call('ZSCORE', keys[4], id) then
    return {'INCONSISTENT'} -- Validate hash and destination indexes before any write.
  end -- Finish validating shared preconditions.

  local nowText = string.format('%.0f', now) -- Avoid losing precision in Redis command arguments.
  local status = 'dead' -- Exhaustion is the default unless another attempt is allowed.
  local runAt = 0 -- Terminal jobs have no retry schedule.
  if attempts < maxAttempts then
    runAt = now + retryDelay(attempts, baseDelay, maxDelay, maxJitter, unitRandom) -- Derive eligibility from the Redis clock.
    status = 'delayed' -- Promotion will make the next attempt claimable when eligible.
    local runAtText = string.format('%.0f', runAt) -- Serialize the full timestamp.
    redis.call('ZADD', keys[3], runAtText, id) -- Schedule the retry before releasing the lease.
    redis.call('HSET', keys[1], 'runAt', runAtText) -- Persist the chosen schedule.
  else -- The job has no remaining attempts.
    redis.call('XADD', keys[5], '*', 'event', 'dead', 'jobId', id, 'queueName', queue,
      'type', job[3], 'payload', job[4], 'replayCount', job[5], 'attempts', job[1], -- Capture the dead generation.
      'lastError', lastError, 'stack', stack, 'deadAt', nowText) -- Enqueue the archive write before changing state.
    redis.call('ZADD', keys[4], nowText, id) -- Index the live dead-letter record.
    redis.call('HSET', keys[1], 'deadAt', nowText) -- Save when the attempt budget was exhausted.
  end -- Finish selecting the destination.

  redis.call('HSET', keys[1], 'status', status, 'lastError', lastError) -- Keep the hash aligned with its index.
  if stack == '' then
    redis.call('HDEL', keys[1], 'stack') -- Clear a previous stack when this failure has none.
  else -- Handler exceptions can carry a stack.
    redis.call('HSET', keys[1], 'stack', stack) -- Preserve the latest stack for diagnosis.
  end -- Finish recording error details.
  redis.call('ZREM', keys[2], id) -- Release ownership without resetting the fencing counter.
  redis.call('HDEL', keys[1], 'workerId', 'claimId') -- Remove the expired or failed claim.
  return {'FAILED', status, runAt, now, attempts, job[9]} -- Return the transition for logging and the ledger.
end -- Finish the shared recovery transition.
