-- replay.lua — DLQ 回放的原子路径（勘误 #6 新增）
--
-- KEYS[1]=task hash  KEYS[2]=ready  KEYS[3]=sched
-- ARGV[1]=task_id  ARGV[2]=payload  ARGV[3]=pri  ARGV[4]=delay_ms
-- ARGV[5]=max_retries  ARGV[6]=headers_json  ARGV[7]=force(0/1)
--
-- 返回: {OK, id} | {ERR_DUP, id} | {ERR_STATE[, st]}
--
-- 回放 = 读 DLQ 快照重新 enqueue（新 tries 周期）。为什么是脚本而不是运维侧
-- 裸 DEL+enqueue：DEL 与重投之间若任务被其他路径触碰（重放重入、误判），裸
-- 命令是 check-then-act 竞态。脚本内原子完成：仅在 state==DLQ（终态，不可能
-- 再转移）时允许 DEL 残留 hash；force=0 且 hash 仍在保留期内 → ERR_DUP，
-- 需调用方显式授权（kikictl dlq replay --force）。在途任务（非 DLQ 态）
-- 一律拒绝，无论 force 与否。

local tk = KEYS[1]
if redis.call('EXISTS', tk) == 1 then
  local st = redis.call('HGET', tk, 'state')
  if st ~= 'DLQ' then return {'ERR_STATE', st} end
  if ARGV[7] ~= '1' then return {'ERR_DUP', ARGV[1]} end
  redis.call('DEL', tk)
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
  redis.call('ZADD', KEYS[3], now + delay, ARGV[1])
else
  redis.call('ZADD', KEYS[2], pri * P48 + now, ARGV[1])
end
return {'OK', ARGV[1]}
