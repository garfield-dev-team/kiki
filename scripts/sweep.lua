-- sweep.lua — 转移 #9/#10（租约回收——韧性核心，无主幂等）
--
-- KEYS[1]=lease  KEYS[2]=ready  KEYS[3]=sched  KEYS[4]=dlq stream
-- ARGV[1]=task key 前缀  ARGV[2]=limit(单轮上限)  ARGV[3]=max_redeliveries
-- ARGV[4]=retention_ms
--
-- 返回: {SWEPT, requeued, dlqd, <dlq 的 task_id...>}
--
-- 三条性质，全部来自"原子脚本 + 全量守卫"：
-- 1. 幂等：任何一步前置检查不满足就走清理分支，多实例并发跑、重复跑结果一致
--    → 无需选主，任何 worker 都能执行，回收器自身无单点；
-- 2. ver+1 是本方案毒杀旧 token 的地方：重投瞬间旧 worker 的 complete/
--    heartbeat 全部 ERR_FENCED（Kleppmann fencing token 的 Redis 落地）；
-- 3. lease_resets 上限是毒丸第二防线：反复杀死 worker 而无人调 fail 的任务
--    （OOM、反序列化炸弹）由此汇入 DLQ，否则在"超时→重投"里无限循环。
--
-- 勘误 #5：重投 score = pri×2^48 + now（回到队尾）。原示意 (2^48-1-now)
-- 在 ZPOPMIN 下会插到队头。XADD 快照含 pri/max_retries/headers（replay 自足）。

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local P48 = 281474976710656
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now,
                           'LIMIT', 0, tonumber(ARGV[2]))
local requeued, dlqd = 0, 0
local dlq_ids = {}
for _, id in ipairs(expired) do
  local tk = ARGV[1] .. id
  if redis.call('EXISTS', tk) == 0 then
    redis.call('ZREM', KEYS[1], id)                    -- 孤儿条目 GC
  elseif redis.call('HGET', tk, 'state') ~= 'RESERVED' then
    redis.call('ZREM', KEYS[1], id)                    -- complete 已赢，清理租约索引
  else
    local resets = redis.call('HINCRBY', tk, 'lease_resets', 1)
    redis.call('HINCRBY', tk, 'ver', 1)                -- ★ 毒杀旧 worker 的 token
    redis.call('HDEL', tk, 'owner', 'lease_deadline')
    redis.call('ZREM', KEYS[1], id)
    if resets > tonumber(ARGV[3]) then
      redis.call('HSET', tk, 'state', 'DLQ', 'last_error', 'lease_exceeded',
                 'updated_ms', now)
      redis.call('XADD', KEYS[4], '*',
        'id', id,
        'payload', redis.call('HGET', tk, 'payload'),
        'headers', redis.call('HGET', tk, 'headers') or '',
        'err', 'lease_exceeded',
        'tries', redis.call('HGET', tk, 'tries'),
        'max_retries', redis.call('HGET', tk, 'max_retries'),
        'pri', redis.call('HGET', tk, 'pri'),
        'via', 'sweep',
        'ts', now)
      redis.call('EXPIRE', tk, tonumber(ARGV[4]))
      dlqd = dlqd + 1
      dlq_ids[#dlq_ids + 1] = id
    else
      local pri = tonumber(redis.call('HGET', tk, 'pri'))
      redis.call('HSET', tk, 'state', 'READY', 'updated_ms', now)
      redis.call('ZADD', KEYS[2], pri * P48 + now, id)  -- 回到队尾
      requeued = requeued + 1
    end
  end
end
local out = {'SWEPT', tostring(requeued), tostring(dlqd)}
for _, id in ipairs(dlq_ids) do out[#out + 1] = id end
return out
