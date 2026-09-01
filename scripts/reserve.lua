-- reserve.lua — 转移 #4（并发控制核心）
--
-- KEYS[1]=ready  KEYS[2]=lease
-- ARGV[1]=worker_id  ARGV[2]=vis_ms  ARGV[3]=maxn(批量)  ARGV[4]=task key 前缀
--
-- 返回: 行数组（可能为空），每行 {id, ver, tries, lease_deadline, payload, headers}
--
-- 双重预订在物理上不可能：ZPOPMIN 在脚本内原子弹出，弹出的瞬间任务已离开
-- ready ZSET。ver 在此处颁发（fencing token）；vis 到期由 sweep 回收（sweep.lua）。
-- task key 经 ARGV 前缀动态拼接：所有 KEYS 共享 hash tag {qk:q}，同 slot，
-- Cluster 下合法（go-implementation.md §3.4，T12 冒烟覆盖）。

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local maxn = tonumber(ARGV[3])
local out = {}
for _ = 1, maxn do
  local p = redis.call('ZPOPMIN', KEYS[1], 1)
  if #p == 0 then break end
  local id = p[1]
  local tk = ARGV[4] .. id
  if redis.call('EXISTS', tk) == 0 then
    redis.call('ZREM', KEYS[2], id)   -- 防御性孤儿 GC，理论上不发生
  else
    local ver   = redis.call('HINCRBY', tk, 'ver', 1)      -- 颁发 fencing token
    local tries = redis.call('HINCRBY', tk, 'tries', 1)
    local dl    = now + tonumber(ARGV[2])
    redis.call('HSET', tk, 'state', 'RESERVED', 'owner', ARGV[1],
               'lease_deadline', dl, 'updated_ms', now)
    redis.call('ZADD', KEYS[2], dl, id)
    out[#out + 1] = {
      id, tostring(ver), tostring(tries), tostring(dl),
      redis.call('HGET', tk, 'payload'),
      redis.call('HGET', tk, 'headers') or '',
    }
  end
end
return out
