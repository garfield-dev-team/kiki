-- heartbeat.lua — 转移 #5（租期续约 + 存活信号）
--
-- KEYS[1]=lease  KEYS[2]=task hash
-- ARGV[1]=worker  ARGV[2]=token(ver)  ARGV[3]=task_id  ARGV[4]=extend_ms
--
-- 返回: {OK, dl} | {ERR_GONE} | {ERR_STATE} | {ERR_OWNER} | {ERR_FENCED}
--
-- 勘误 #1（go-implementation.md §3.5）：lease ZSET 的 member 恒为 task_id。
-- design.md §5.3 示意误写为 token（ARGV[2]），member 若是 token 则 sweep
-- 永远扫不到被续期的任务（sweep 按 task_id 找 task key、并按 task_id ZREM）。

local tk = KEYS[2]
if redis.call('EXISTS', tk) == 0 then return {'ERR_GONE'} end
if redis.call('HGET', tk, 'state') ~= 'RESERVED' then return {'ERR_STATE'} end
if redis.call('HGET', tk, 'owner') ~= ARGV[1] then return {'ERR_OWNER'} end
if tonumber(redis.call('HGET', tk, 'ver')) ~= tonumber(ARGV[2]) then
  return {'ERR_FENCED'}
end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local dl = now + tonumber(ARGV[4])
redis.call('HSET', tk, 'lease_deadline', dl, 'hb_ms', now, 'updated_ms', now)
redis.call('ZADD', KEYS[1], dl, ARGV[3])
return {'OK', tostring(dl)}
