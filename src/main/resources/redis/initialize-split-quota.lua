-- Operator-only: stop and drain ALL senders before switching policy. Never overwrite existing state.
if #KEYS ~= 1 or #ARGV ~= 4 then return redis.error_reply('QUOTA_INVALID_INITIALIZATION') end
local key = KEYS[1]
local rate, burst = tonumber(ARGV[1]), tonumber(ARGV[2])
local cr, cb = tonumber(ARGV[3]), tonumber(ARGV[4])
if #key < 1 or #key > 128 or not string.match(key, '^[%w:_%-]+$')
        or not rate or not burst or not cr or not cb
        or rate < 2 or rate > 10000 or burst < 2 or burst > 10000
        or cr < 1 or cr >= rate or cb < 1 or cb >= burst
        or rate ~= math.floor(rate) or burst ~= math.floor(burst)
        or cr ~= math.floor(cr) or cb ~= math.floor(cb) then
    return redis.error_reply('QUOTA_INVALID_INITIALIZATION')
end
if redis.call('EXISTS', key) == 1 then return redis.error_reply('QUOTA_ALREADY_EXISTS') end
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
redis.call('HSET', key, 'version', '3', 'rate', rate, 'burst', burst,
    'campaign_rate', cr, 'campaign_burst', cb, 'realtime_credit', 0, 'campaign_credit', 0, 'last_ms', now)
return redis.status_reply('QUOTA_INITIALIZED_EMPTY')
