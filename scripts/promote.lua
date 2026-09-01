-- promote.lua — 转移 #3（SCHEDULED → READY，到期提升）
--
-- KEYS[1]=sched  KEYS[2]=ready  KEYS[3]=task key 前缀
-- ARGV[1]=limit（单轮上限）
--
-- 返回: {PROMOTED, n, <task_id...>}
--
-- 与 sweep 同周期执行，时间一律取自 Redis 服务器（TIME）。
-- 逐个校验 state==SCHEDULED；ZREM 原子移除 + 全量守卫 ⇒ 与 sweep 一样
-- 幂等无主，多实例并发跑无害。
-- ready score = pri×2^48 + visible_at（原 sched score，勘误 #5 编码）：
-- 同优先级按到期序提升，且与 ready 中按 enqueue 序/队尾序编码的条目自然融合
-- （等得更久的任务分值更小、先被弹出）。

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local P48 = 281474976710656
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now,
                       'WITHSCORES', 'LIMIT', 0, tonumber(ARGV[1]))
local n = 0
local promoted_ids = {}
for i = 1, #due, 2 do
  local id = due[i]
  local visible_at = math.floor(tonumber(due[i + 1]))
  local tk = KEYS[3] .. id
  if redis.call('EXISTS', tk) == 0 then
    redis.call('ZREM', KEYS[1], id)                    -- 孤儿 GC
  elseif redis.call('HGET', tk, 'state') ~= 'SCHEDULED' then
    redis.call('ZREM', KEYS[1], id)                    -- 状态已漂移，清理 sched 条目
  else
    local pri = tonumber(redis.call('HGET', tk, 'pri'))
    redis.call('HSET', tk, 'state', 'READY', 'updated_ms', now)
    redis.call('ZADD', KEYS[2], pri * P48 + visible_at, id)
    redis.call('ZREM', KEYS[1], id)
    n = n + 1
    promoted_ids[#promoted_ids + 1] = id
  end
end
local out = {'PROMOTED', tostring(n)}
for _, id in ipairs(promoted_ids) do out[#out + 1] = id end
return out
