-- One provider key, two non-borrowing reservations. Their rate/burst sums equal the provider envelope.
local key = KEYS[1]
local rate, burst = tonumber(ARGV[1]), tonumber(ARGV[2])
local cr, cb = tonumber(ARGV[3]), tonumber(ARGV[4])
local lane = ARGV[5]
if #KEYS ~= 1 or #ARGV ~= 5 or (lane ~= 'REALTIME' and lane ~= 'CAMPAIGN')
        or not rate or not burst or not cr or not cb
        or rate < 2 or rate > 10000 or burst < 2 or burst > 10000
        or cr < 1 or cr >= rate or cb < 1 or cb >= burst
        or rate ~= math.floor(rate) or burst ~= math.floor(burst)
        or cr ~= math.floor(cr) or cb ~= math.floor(cb) then
    return redis.error_reply('QUOTA_POLICY_MISMATCH')
end
if redis.call('EXISTS', key) == 0 then return redis.error_reply('QUOTA_NOT_INITIALIZED') end
if redis.call('PTTL', key) ~= -1 then return redis.error_reply('QUOTA_UNEXPECTED_EXPIRY') end
local s = redis.call('HMGET', key, 'version', 'rate', 'burst', 'campaign_rate', 'campaign_burst',
    'realtime_credit', 'campaign_credit', 'last_ms')
if s[1] ~= '3' or tonumber(s[2]) ~= rate or tonumber(s[3]) ~= burst
        or tonumber(s[4]) ~= cr or tonumber(s[5]) ~= cb then
    return redis.error_reply('QUOTA_POLICY_MISMATCH')
end
local rt, cp, last = tonumber(s[6]), tonumber(s[7]), tonumber(s[8])
local rr, rb = rate - cr, burst - cb
if not rt or not cp or not last or rt < 0 or rt > rb * 1000 or cp < 0 or cp > cb * 1000
        or rt ~= math.floor(rt) or cp ~= math.floor(cp)
        or last < 0 or last > 9007199254740991 or last ~= math.floor(last) then
    return redis.error_reply('QUOTA_INVALID_STATE')
end
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local elapsed = math.max(0, now - last)
rt = math.min(rb * 1000, rt + math.min(elapsed, math.ceil(rb * 1000 / rr)) * rr)
cp = math.min(cb * 1000, cp + math.min(elapsed, math.ceil(cb * 1000 / cr)) * cr)
last = math.max(last, now)
local credit, refill = rt, rr
if lane == 'CAMPAIGN' then credit, refill = cp, cr end
local admitted, retry = 0, 0
if credit >= 1000 then
    admitted = 1
    if lane == 'CAMPAIGN' then cp = cp - 1000 else rt = rt - 1000 end
else
    retry = last - now + math.ceil((1000 - credit) / refill)
end
redis.call('HSET', key, 'realtime_credit', rt, 'campaign_credit', cp, 'last_ms', last)
return {admitted, retry}
