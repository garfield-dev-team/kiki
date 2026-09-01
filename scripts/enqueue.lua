-- enqueue.lua — 转移 #1/#2（生产者幂等）
--
-- KEYS[1]=task hash  KEYS[2]=ready  KEYS[3]=sched
-- ARGV[1]=task_id  ARGV[2]=payload  ARGV[3]=pri(0最高)  ARGV[4]=delay_ms
-- ARGV[5]=max_retries  ARGV[6]=headers_json(可为空串)
--
-- 返回: {OK, id} | {ERR_DUP, id}
--
-- 生产者重试的幂等防线：业务 id 作任务 id，EXISTS 命中即拒。
-- ready score = pri×2^48 + now_ms（勘误 #5：ZPOPMIN 取最小分值，
-- 该编码下 pri 越小越优先、同优先级 FIFO）。
-- 与 design.md §5.1 示意的差异：不接收 retention_ms（enqueue 无过期语义）；
-- 增加 headers 与 sv（schema version，见 go-implementation.md §12）。

local tk = KEYS[1]
if redis.call('EXISTS', tk) == 1 then
  return {'ERR_DUP', ARGV[1]}
end

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local pri = tonumber(ARGV[3])
local delay = tonumber(ARGV[4])

redis.call('HSET', tk,
  'id', ARGV[1],
  'payload', ARGV[2],
  'pri', pri,
  'state', delay > 0 and 'SCHEDULED' or 'READY',
  'tries', 0,
  'ver', 0,
  'max_retries', tonumber(ARGV[5]),
  'headers', ARGV[6],
  'sv', 1,
  'created_ms', now,
  'updated_ms', now)

local P48 = 281474976710656
if delay > 0 then
  redis.call('ZADD', KEYS[3], now + delay, ARGV[1])   -- sched score = visible_at
else
  redis.call('ZADD', KEYS[2], pri * P48 + now, ARGV[1])
end
return {'OK', ARGV[1]}
