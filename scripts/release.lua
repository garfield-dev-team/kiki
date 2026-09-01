-- release.lua — 转移 #11（优雅下线：不计失败，立即可被接班 worker 领取）
--
-- KEYS[1]=lease  KEYS[2]=ready  KEYS[3]=task hash
-- ARGV[1]=worker  ARGV[2]=token  ARGV[3]=task_id
--
-- 返回: {OK} | {ERR_GONE} | {ERR_STATE[, st]} | {ERR_FENCED}
--
-- 语义 = fail 的重试分支但 tries 不增加、不 bump ver、err 置 released：
-- 下线不是失败，轮到谁处理任务就由谁处理；fencing 纪律规定 ver 只在
-- sweep / fail-retry 失效，release 不在其列（state 守卫已挡住幽灵写）。
-- 回 ready 队尾（score = pri×2^48 + now），避免与未处理过的任务抢队头。

local tk = KEYS[3]
if redis.call('EXISTS', tk) == 0 then return {'ERR_GONE'} end
local st = redis.call('HGET', tk, 'state')
if st ~= 'RESERVED' then return {'ERR_STATE', st} end
local same = redis.call('HGET', tk, 'owner') == ARGV[1]
             and tonumber(redis.call('HGET', tk, 'ver')) == tonumber(ARGV[2])
if not same then return {'ERR_FENCED'} end

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local id  = redis.call('HGET', tk, 'id')
local pri = tonumber(redis.call('HGET', tk, 'pri'))
local P48 = 281474976710656
redis.call('HSET', tk, 'state', 'READY', 'last_error', 'released', 'updated_ms', now)
redis.call('ZADD', KEYS[2], pri * P48 + now, id)
redis.call('ZREM', KEYS[1], id)
return {'OK'}
