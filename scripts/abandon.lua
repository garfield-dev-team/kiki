-- abandon.lua — 非 retryable 立即死信（go-implementation.md §6.2 新增，勘误 #3）
--
-- KEYS[1]=lease  KEYS[2]=dlq stream  KEYS[3]=task hash
-- ARGV[1]=worker  ARGV[2]=token  ARGV[3]=task_id  ARGV[4]=err  ARGV[5]=retention_ms
--
-- 返回: {DLQ, tries} | {ERR_GONE} | {ERR_STATE[, st]} | {ERR_FENCED}
--
-- design.md fail 分岔只在 tries ≥ 上限时进 DLQ；NonRetryable 错误（参数非法、
-- 4xx 类）重试纯属浪费，守卫同 fail（state==RESERVED + owner + token），
-- 命中后直接走 DLQ 分支，via='abandon'。不 bump ver（fencing 纪律）。

local tk = KEYS[3]
if redis.call('EXISTS', tk) == 0 then return {'ERR_GONE'} end
local st = redis.call('HGET', tk, 'state')
if st ~= 'RESERVED' then return {'ERR_STATE', st} end
local same = redis.call('HGET', tk, 'owner') == ARGV[1]
             and tonumber(redis.call('HGET', tk, 'ver')) == tonumber(ARGV[2])
if not same then return {'ERR_FENCED'} end

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local id = redis.call('HGET', tk, 'id')
redis.call('HSET', tk, 'state', 'DLQ', 'last_error', ARGV[4], 'updated_ms', now)
redis.call('XADD', KEYS[2], '*',
  'id', id,
  'payload', redis.call('HGET', tk, 'payload'),
  'headers', redis.call('HGET', tk, 'headers') or '',
  'err', ARGV[4],
  'tries', redis.call('HGET', tk, 'tries'),
  'max_retries', redis.call('HGET', tk, 'max_retries'),
  'pri', redis.call('HGET', tk, 'pri'),
  'via', 'abandon',
  'ts', now)
redis.call('ZREM', KEYS[1], id)
redis.call('EXPIRE', tk, tonumber(ARGV[5]))
return {'DLQ', redis.call('HGET', tk, 'tries')}
