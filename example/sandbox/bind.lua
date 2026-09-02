-- bind.lua —— Dispatcher 的原子配对（rendezvous）脚本
-- （application-level：agent-sandbox-task-queue.md §二.2 指定的"原方案之外唯一需要新写的小脚本"）
--
-- 解决"两个队列的会合"：请求（等待室）× warm 容器（资源池）单脚本内原子配对，
-- 两个用户绝不可能拿到同一个容器；池空就不领，请求原地排队。
--
-- 对容器侧执行与 kiki reserve.lua 相同的状态转移（ver/tries/owner/lease），
-- 使心跳续约、僵尸 sweep、fencing token、lease_resets 毒丸防线原样作用于"被租用的资源"。
--
-- KEYS[1]=req ready ZSET   KEYS[2]=pool ready ZSET   KEYS[3]=pool lease ZSET   KEYS[4]=bindings HASH
-- ARGV[1]=session_id（dispatcher 为队头请求现场生成）
-- ARGV[2]=vis_ms（会话租期）
-- ARGV[3]=req task key 前缀   ARGV[4]=pool task key 前缀
--
-- 返回: {BIND, req_id, container_id, addr, token, lease_deadline} | {EMPTY}
--
-- 诚实条款：req 与 pool 是不同 hash tag ⇒ 跨 slot 脚本仅在单机/主从 Redis 合法；
-- 生产 Cluster 下需让请求队列与资源池同 slot 对齐（或拆成两段脚本 + 补偿），见文档 §四.1。

-- 1. 等待室队头（score = pri×2^48 + ts，pri 越小越优先 ⇒ 会员等级）
local req_id = redis.call('ZRANGE', KEYS[1], 0, 0)[1]
if not req_id then return {'EMPTY'} end

-- 2. 池空不领：请求原地排队（前端可用 ZRANK 免费送排队位次）
local popped = redis.call('ZPOPMIN', KEYS[2], 1)
if #popped == 0 then return {'EMPTY'} end
local cid = popped[1]

-- 3. 容器侧：与 reserve.lua 相同的领取转移
local tk = ARGV[4] .. cid
if redis.call('EXISTS', tk) == 0 then
  redis.call('ZREM', KEYS[3], cid)          -- 防御性孤儿 GC
  return {'EMPTY'}
end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local ver = redis.call('HINCRBY', tk, 'ver', 1)   -- ★ fencing token：本次租约的访问凭据
local tries = redis.call('HINCRBY', tk, 'tries', 1) -- 该容器累计被分配次数
local dl = now + tonumber(ARGV[2])
redis.call('HSET', tk, 'state', 'RESERVED', 'owner', ARGV[1],
           'lease_deadline', dl, 'updated_ms', now)
redis.call('ZADD', KEYS[3], dl, cid)

-- 4. 请求侧：出队并标记已被配对
redis.call('ZPOPMIN', KEYS[1], 1)
redis.call('HSET', ARGV[3] .. req_id, 'state', 'RESERVED',
           'owner', ARGV[1], 'updated_ms', now)

-- 5. 绑定表：session → 容器（网关据此校验后续每一次 exec/代理请求的 token）
local addr = redis.call('HGET', tk, 'payload')
redis.call('HSET', KEYS[4], ARGV[1], cid)

return {'BIND', req_id, cid, addr, tostring(ver), tostring(dl)}
