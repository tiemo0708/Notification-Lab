-- Operator-only initialization. Stop/drain all participating senders first.
-- This script never deletes, refills, migrates, or overwrites an existing budget.
if #KEYS ~= 1 or #ARGV ~= 2 then
    return redis.error_reply('QUOTA_INVALID_INITIALIZATION')
end
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
if #key < 1 or #key > 128 or not string.match(key, '^[%w:_%-]+$')
        or not rate or rate < 1 or rate > 10000 or rate ~= math.floor(rate)
        or not burst or burst < 1 or burst > 10000 or burst ~= math.floor(burst) then
    return redis.error_reply('QUOTA_INVALID_INITIALIZATION')
end
if redis.call('EXISTS', key) == 1 then
    return redis.error_reply('QUOTA_ALREADY_EXISTS')
end
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
redis.call('HSET', key, 'version', '2', 'rate', rate, 'burst', burst, 'credit', 0, 'last_ms', now)
return redis.status_reply('QUOTA_INITIALIZED_EMPTY')
