-- fail.lua — 转移 #7/#8（重试与死信分岔）
--
-- KEYS[1]=lease  KEYS[2]=sched  KEYS[3]=dlq stream  KEYS[4]=task hash
-- ARGV[1]=worker  ARGV[2]=token  ARGV[3]=task_id  ARGV[4]=err
-- ARGV[5]=backoff_ms  ARGV[6]=retention_ms
--
-- 返回: {RETRY, tries, max_retries} | {DLQ, tries, max_retries}
--     | {ERR_GONE} | {ERR_STATE[, st]} | {ERR_FENCED}
--
-- 勘误 #4：DLQ 判据为 tries > max_retries（MaxRetries 语义 = 首投之外的
-- 额外重试上限，共 max_retries+1 次投递），与黄金用例 T7 一致；
-- design.md §5.5 示意的 tries >= maxr 已同步修正。
-- ver+1 只发生在重试分支（fencing 纪律：ver 只在 sweep / fail-retry 失效）。
-- backoff_ms 由客户端计算传入（指数退避+抖动在客户端，机制在脚本）。

local tk = KEYS[4]
if redis.call('EXISTS', tk) == 0 then return {'ERR_GONE'} end
local st = redis.call('HGET', tk, 'state')
if st ~= 'RESERVED' then return {'ERR_STATE', st} end
local same = redis.call('HGET', tk, 'owner') == ARGV[1]
             and tonumber(redis.call('HGET', tk, 'ver')) == tonumber(ARGV[2])
if not same then return {'ERR_FENCED'} end

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local id    = redis.call('HGET', tk, 'id')
local tries = tonumber(redis.call('HGET', tk, 'tries'))
local maxr  = tonumber(redis.call('HGET', tk, 'max_retries'))
redis.call('HSET', tk, 'last_error', ARGV[4], 'updated_ms', now)

if tries > maxr then
  redis.call('HSET', tk, 'state', 'DLQ')
  redis.call('XADD', KEYS[3], '*',
    'id', id,
    'payload', redis.call('HGET', tk, 'payload'),
    'headers', redis.call('HGET', tk, 'headers') or '',
    'err', ARGV[4],
    'tries', tries,
    'max_retries', maxr,
    'pri', redis.call('HGET', tk, 'pri'),
    'via', 'fail',
    'ts', now)
  redis.call('ZREM', KEYS[1], id)
  redis.call('EXPIRE', tk, tonumber(ARGV[6]))
  return {'DLQ', tostring(tries), tostring(maxr)}
end

redis.call('HINCRBY', tk, 'ver', 1)              -- 终结本轮租约的 token
redis.call('HSET', tk, 'state', 'SCHEDULED')
redis.call('HDEL', tk, 'owner', 'lease_deadline')
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[5]), id)   -- sched score = visible_at
redis.call('ZREM', KEYS[1], id)
return {'RETRY', tostring(tries), tostring(maxr)}
