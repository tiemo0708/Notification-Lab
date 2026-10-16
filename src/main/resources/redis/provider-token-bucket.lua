-- One provider key, one token per call. Credit uses 1/1000 token units, so
-- integer rate tokens/second == rate credit/millisecond without rounding loss.
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local capacity = burst * 1000
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local credit = capacity
local last = now

if redis.call('EXISTS', key) == 1 then
    local state = redis.call('HMGET', key, 'version', 'rate', 'burst', 'credit', 'last_ms')
    if state[1] ~= '1' or tonumber(state[2]) ~= rate or tonumber(state[3]) ~= burst then
        return redis.error_reply('QUOTA_POLICY_MISMATCH')
    end
    credit = tonumber(state[4])
    last = tonumber(state[5])
    if not credit or not last or credit < 0 or credit > capacity or credit ~= math.floor(credit)
            or last < 0 or last > 9007199254740991 or last ~= math.floor(last) then
        return redis.error_reply('QUOTA_INVALID_STATE')
    end
end

-- A backwards server-clock step cannot refill or move the stored clock backwards.
-- Cap elapsed before multiplication; idle time cannot accumulate beyond burst.
local elapsed = math.min(math.max(0, now - last), math.ceil(capacity / rate))
credit = math.min(capacity, credit + elapsed * rate)
last = math.max(last, now)
local admitted = 0
local retry_after = 0
if credit >= 1000 then
    credit = credit - 1000
    admitted = 1
else
    retry_after = last - now + math.ceil((1000 - credit) / rate)
end

-- No TTL: this is a single bounded provider bucket, not a per-user key set.
-- Expiry must not erase an exhausted budget or a conflicting policy.
redis.call('HSET', key, 'version', '1', 'rate', rate, 'burst', burst, 'credit', credit, 'last_ms', last)
return {admitted, retry_after}
