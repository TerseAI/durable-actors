local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local rate, burst = tonumber(ARGV[1]), tonumber(ARGV[2])
local previous = redis.call('HMGET', KEYS[1], 'tokens', 'updated')
local updated = tonumber(previous[2]) or now
local tokens = math.min(burst, (tonumber(previous[1]) or burst) + math.max(0, now - updated) * rate / 1000)
if tokens < 1 then return 0 end
redis.call('HSET', KEYS[1], 'tokens', tokens - 1, 'updated', math.max(now, updated))
redis.call('PEXPIRE', KEYS[1], math.max(1000, math.ceil(burst / rate * 1000)))
return 1
