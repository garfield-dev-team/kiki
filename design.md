# 生产级任务队列（Production-Grade Job Queue）技术方案设计

## 0. 设计立场

先说三句丑话，这是整个方案的地基：

1. **本系统保证 at-least-once，不保证 exactly-once。** 在 worker 与队列不是同一个事务域的前提下，exactly-once 是谎言。我们能做的是"effectively-once"：投递可重复，副作用由消费侧幂等键收敛。
2. **`fail` 是事件，不是状态。** 任务失败后要么带着退避重新排队，要么进 DLQ。设计一个没人再驱动它的 `FAILED` 静态，只会制造僵尸。失败原因持久化在任务上（`last_error`），不持久化在状态机上。
3. **所有状态转移必须是单脚本原子操作。** 检查再行动（check-then-act）在分布式队列里必然产生竞态；我们的规则是：任何一个读-改-写序列，要么整体发生，要么不发生。每个脚本就是规范。

---

## 1. 目标与非目标

**目标**：核心操作 `enqueue / reserve / complete / fail`（外加 `heartbeat / release`，没有它们租约模型不成立）；优先级；延迟任务；租约超时自动重投；重试上限 + DLQ 防毒丸；优雅下线。

**非目标**：任务级 DAG 编排、跨队列事务、任务体路由规则。这些是排队系统之上的层。

## 2. 总体架构

**没有中心 broker 进程。Redis 就是 broker。** 三类角色：

```
┌──────────────┐  reserve/heartbeat/complete/fail   ┌─────────────────┐
│  Producer     │ ────enqueue(幂等键)──────────────► │                 │
└──────────────┘                                     │      Redis       │
┌──────────────┐  reserve/heartbeat/complete/fail    │  ZSET×3 + HASH   │
│  Worker 池    │ ◄────────────────────────────────► │  (Lua 原子转移)  │
│ (内嵌 sweeper)│                                     └─────────────────┘
└──────────────┘
```

- **Worker 内嵌 sweeper**：租约回收器不是一个独立服务，而是一段所有 worker 周期性执行、且**幂等**的 Lua 脚本。不需要选主——多跑、并发跑都无害（第 7 节解释为什么）。这是韧性的关键决策：**回收器自身不能是单点**。
- Redis 配置：AOF `everysec` + 主从 + Sentinel（韧性细节见第 9 节）。

## 3. 状态机

```
                    enqueue(delay>0)
                    ┌──────────────┐
                    ▼              │ promote (到期)
 [生产者] ──enqueue──► SCHEDULED ───┴──► READY ──reserve──► RESERVED ──complete──► COMPLETED (终态)
                                       ▲ ▲                 │ ││
                        release(优雅下线)│                 │ │└─ fail(tries≥max) ──► DLQ (终态)
                        sweep(租约到期) ─┘                 │ └── fail(tries<max, 指数退避)
                                                          └───── sweep(租约到期且重投超限)
```

**转移表（这才是规范，图只是它的投影）：**

| # | 当前态 | 事件 | 守卫 | 次态 | 副作用 |
|---|---|---|---|---|---|
| 1 | 无 | enqueue, delay=0 | 任务 id 不存在 | READY | ZADD ready |
| 2 | 无 | enqueue, delay>0 | 任务 id 不存在 | SCHEDULED | ZADD sched(visible_at) |
| 3 | SCHEDULED | promote（now ≥ visible_at） | — | READY | sched → ready，按到期序 |
| 4 | READY | reserve | 队头可弹 | RESERVED | **tries+1，ver+1，记 owner**，ZADD lease(now+vis) |
| 5 | RESERVED | heartbeat | owner+token 匹配 | RESERVED | 租约 deadline 后移 |
| 6 | RESERVED | complete | owner+token 匹配 | COMPLETED | ZREM lease，任务保留期后过期 |
| 7 | RESERVED | fail, tries ≤ max_retries | owner+token 匹配 | SCHEDULED | 指数退避入 sched，**ver+1** |
| 8 | RESERVED | fail, tries > max_retries | owner+token 匹配 | DLQ | XADD DLQ（含 payload/err/tries），保留期后过期 |
| 9 | RESERVED | sweep（deadline < now） | state 仍为 RESERVED | READY | **ver+1**（毒杀旧 token），requeue 队尾（score=pri×2⁴⁸+now） |
| 10 | RESERVED | sweep + redeliveries 超限 | lease_resets > 上限 | DLQ | 同 #8，err=`lease_exceeded` |
| 11 | RESERVED | release（优雅下线） | owner+token 匹配 | READY | 不计失败，requeue |
| 12 | COMPLETED/DLQ | 保留期到 | — | （key 过期） | 历史由 DLQ Stream 长期留存 |

与题面的对应：`Unassigned` = READY/SCHEDULED；`Completed` 对应 #6；`Failed` 不是停留态——它分解为 #7（重试）与 #8（进 DLQ）。

## 4. 数据结构选型

### 4.1 为什么是 ZSET，不是内存最小堆

堆是**进程内**结构：随进程死掉、无法被多实例共享、无法持久化与复制。队列的超时索引必须满足：① 所有角色对同一份倒序视图达成一致；② 崩溃后仍在。Redis Sorted Set 每项 O(log N) 插入/弹出，`ZRANGEBYSCORE` 天然就是"到期任务扫描器"，且随 Redis 持久化/主从存活。堆只有在单进程内存队列（如 Go channel + timer heap）里才成立，那不是本方案的目标形态。

### 4.2 为什么不直接用 Redis Streams + Consumer Group

诚实评估：`XREADGROUP + XAUTOCLAIM`（6.2+）已内置 PEL 与租约转移，能覆盖 60% 需求。放弃它的原因：① 无优先级；② 每任务独立的重试上限与退避策略需要额外记账结构，最终还是要配 HASH；③ DLQ 的投递语义与回放控制需要自己搭。我们的方案本质上是把 PEL 思想一般化并显式化——既然要显式，不如全显式。

### 4.3 Key 布局（以队列 `q` 为例）

| Key | 类型 | 内容 | score 语义 |
|---|---|---|---|
| `{qk:q}:ready` | ZSET | member=task_id | `pri×2⁴⁸ + ts_ms`（ts ∈ {enqueue_ms, 重投 now, promoted visible_at}），**pri 越小越优先，同优先级 FIFO（ZPOPMIN 取最小分值）** |
| `{qk:q}:sched` | ZSET | member=task_id | `visible_at_ms`（延迟任务 + 重试退避共用） |
| `{qk:q}:lease` | ZSET | member=task_id | `lease_deadline_ms`（租约/可见性超时索引） |
| `{qk:q}:t:<id>` | HASH | payload, state, pri, tries, ver, owner, lease_deadline, lease_resets, last_error, created_ms… | — |
| `{qk:q}:dlq` | Stream | 完整任务快照 + 失败上下文 | 支持 XRANGE 回放 |

三个工程要点：

- **Fencing token 就是 HASH 里的 `ver` 字段**（单调递增版本号），不是额外系统。
- **所有 key 共享 hash tag `{qk:q}`** → 同一队列落在同一 cluster slot。代价是单队列吞吐上限 = 单分片上限；热队列用 `qk:q:0..N` 子分片横向扩展，客户端合并视图。
- **`reserve`/`sweep` 弹出前不知道 task_id**，脚本内动态拼 task key。同 slot 约束使这在 Cluster 下合法；单机模式则完全无限制。

### 4.4 为什么租约索引用 ZSET 而不是 key TTL + keyspace notification

TTL 过期是惰性/抽样删除，**不保证准时、更不保证回调送达**（notification 是 fire-and-forget，客户端断线就丢）。租约超时是本系统的核心正确性机制，不能建立在"尽力而为"上。ZSET score 扫描是确定性的：到期的任务一定在扫描结果里，错过这轮下轮还在。

## 5. 核心操作：Lua 原子实现

返回约定：所有脚本返回 `{STATUS, ...}`，STATUS ∈ `OK / OK_DUP / RETRY / DLQ / ERR_STATE / ERR_OWNER / ERR_FENCED / ERR_GONE / ERR_DUP`。`ERR_FENCED` 意为"你的 token 已过期，租约已易主"——客户端必须停止一切后续动作，且假设副作用可能已被人重做。

时间一律取自 Redis 服务器（脚本内 `TIME`），**客户端时钟只用来写日志**。Redis 5+ 脚本按效果复制，脚本内 `TIME` 安全。

### 5.1 enqueue（转移 #1/#2，生产者幂等）

```lua
-- KEYS[1]=task hash  KEYS[2]=ready  KEYS[3]=sched
-- ARGV[1]=task_id ARGV[2]=payload ARGV[3]=pri(0最高) ARGV[4]=delay_ms
-- ARGV[5]=max_retries ARGV[6]=retention_ms
local id, tk = ARGV[1], KEYS[1]
if redis.call('EXISTS', tk) == 1 then return {'ERR_DUP', id} end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local pri, delay = tonumber(ARGV[3]), tonumber(ARGV[4])
redis.call('HSET', tk, 'id', id, 'payload', ARGV[2], 'pri', pri,
  'state', 'READY', 'tries', 0, 'ver', 0,
  'max_retries', tonumber(ARGV[5]), 'created_ms', now, 'updated_ms', now)
if delay > 0 then
  redis.call('HSET', tk, 'state', 'SCHEDULED')
  redis.call('ZADD', KEYS[3], now + delay, id)
else
  redis.call('ZADD', KEYS[2], pri * 281474976710656 + now, id)
end
return {'OK', id}
```

任务 id 由**生产者的业务唯一键**充当，`EXISTS` 检查即生产者重试的幂等防线。这是接口所有权问题：队列不生成 id，因为它无法知道业务上的"同一个任务"是什么。

### 5.2 reserve（转移 #4，并发控制核心）

```lua
-- KEYS[1]=ready  KEYS[2]=lease
-- ARGV[1]=worker_id ARGV[2]=vis_ms(可见性超时) ARGV[3]=maxn(批量) ARGV[4]=task_key前缀
local ready, lease = KEYS[1], KEYS[2]
local worker, vis, maxn = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local out = {}
for i = 1, maxn do
  local p = redis.call('ZPOPMIN', ready, 1)
  if #p == 0 then break end
  local id = p[1]
  local tk = ARGV[4] .. id
  if redis.call('EXISTS', tk) == 0 then
    redis.call('ZREM', lease, id)   -- 防御性 GC，理论上不发生
  else
    local ver   = redis.call('HINCRBY', tk, 'ver', 1)      -- 颁发 fencing token
    local tries = redis.call('HINCRBY', tk, 'tries', 1)
    local dl    = now + vis
    redis.call('HSET', tk, 'state', 'RESERVED', 'owner', worker,
               'lease_deadline', dl, 'updated_ms', now)
    redis.call('ZADD', lease, dl, id)
    out[#out+1] = {id, tostring(ver), tostring(tries), redis.call('HGET', tk, 'payload')}
  end
end
return out
```

- **双重预订在物理上不可能**：`ZPOPMIN` 在脚本内原子执行，同一任务只可能被弹出一次；弹出的瞬间它已离开 ready ZSET。
- vis_ms 由 worker 声明（受队列上限约束），心跳续约（5.3）是长任务的正道，而不是把 vis 调到 10 分钟——vis 过大意味着 worker 崩溃后系统停摆 10 分钟。
- 返回的 `ver` 就是 fencing token，随任务下发给 worker 持有。

### 5.3 heartbeat（转移 #5，租期续约 + 存活信号）

```lua
-- KEYS[1]=lease  KEYS[2]=task hash
-- ARGV[1]=worker ARGV[2]=token(ver) ARGV[3]=extend_ms
local tk = KEYS[2]
if redis.call('EXISTS', tk) == 0 then return {'ERR_GONE'} end
if redis.call('HGET', tk, 'state') ~= 'RESERVED' then return {'ERR_STATE'} end
if redis.call('HGET', tk, 'owner') ~= ARGV[1] then return {'ERR_OWNER'} end
if tonumber(redis.call('HGET', tk, 'ver')) ~= tonumber(ARGV[2]) then return {'ERR_FENCED'} end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local dl = now + tonumber(ARGV[3])
redis.call('HSET', tk, 'lease_deadline', dl, 'hb_ms', now, 'updated_ms', now)
redis.call('ZADD', KEYS[1], dl, ARGV[2])
return {'OK', tostring(dl)}
```

建议心跳间隔 = vis/3：丢一次心跳不至于触发超时，连续两次丢失就应告警。

### 5.4 complete（转移 #6，幂等 + 幽灵写防护）

```lua
-- KEYS[1]=lease  KEYS[2]=task hash
-- ARGV[1]=worker ARGV[2]=token ARGV[3]=result ARGV[4]=retention_ms
local tk = KEYS[2]
if redis.call('EXISTS', tk) == 0 then return {'ERR_GONE'} end
local st = redis.call('HGET', tk, 'state')
local same = redis.call('HGET', tk, 'owner') == ARGV[1]
             and tonumber(redis.call('HGET', tk, 'ver')) == tonumber(ARGV[2])
if st == 'COMPLETED' and same then return {'OK_DUP'} end   -- 响应丢失后的重试
if st ~= 'RESERVED' then return {'ERR_STATE', st} end
if not same then return {'ERR_FENCED'} end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('HSET', tk, 'state', 'COMPLETED', 'result', ARGV[3],
           'finished_ms', now, 'updated_ms', now)
redis.call('ZREM', KEYS[1], redis.call('HGET', tk, 'id'))
redis.call('EXPIRE', tk, tonumber(ARGV[4]))
return {'OK'}
```

一个关键决策要讲清楚：**complete 不检查租约是否已过期，只检查 owner+token**。理由：租约易主只能经 sweep 发生，sweep 会 `ver+1`；如果 token 仍匹配，说明 sweep 尚未触碰此任务，此刻没有人可能持有它——接受这个 complete 是线性化正确的，且避免了"任务其实已处理完却被重投"的重复执行。Lua 脚本域内的串行化让 complete 与 sweep 必然一先一后，不存在中间态。

### 5.5 fail（转移 #7/#8，重试与死信分岔）

```lua
-- KEYS[1]=lease KEYS[2]=sched KEYS[3]=dlq stream KEYS[4]=task hash
-- ARGV[1]=worker ARGV[2]=token ARGV[3]=err ARGV[4]=backoff_ms ARGV[5]=retention_ms
local tk = KEYS[4]
if redis.call('EXISTS', tk) == 0 then return {'ERR_GONE'} end
if redis.call('HGET', tk, 'state') ~= 'RESERVED' then return {'ERR_STATE'} end
local same = redis.call('HGET', tk, 'owner') == ARGV[1]
             and tonumber(redis.call('HGET', tk, 'ver')) == tonumber(ARGV[2])
if not same then return {'ERR_FENCED'} end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local id   = redis.call('HGET', tk, 'id')
local tries = tonumber(redis.call('HGET', tk, 'tries'))
local maxr  = tonumber(redis.call('HGET', tk, 'max_retries'))
redis.call('HSET', tk, 'last_error', ARGV[3], 'updated_ms', now)
if tries > maxr then
  redis.call('HSET', tk, 'state', 'DLQ')
  redis.call('XADD', KEYS[3], '*', 'id', id,
    'payload', redis.call('HGET', tk, 'payload'),
    'err', ARGV[3], 'tries', tries, 'via', 'fail', 'ts', now)
  redis.call('ZREM', KEYS[1], id)
  redis.call('EXPIRE', tk, tonumber(ARGV[5]))
  return {'DLQ', tostring(tries), tostring(maxr)}
end
redis.call('HINCRBY', tk, 'ver', 1)                    -- 终结本轮租约的 token
redis.call('HSET', tk, 'state', 'SCHEDULED')
redis.call('HDEL', tk, 'owner', 'lease_deadline')
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[4]), id) -- 指数退避+抖动由客户端算好传入
redis.call('ZREM', KEYS[1], id)
return {'RETRY', tostring(tries), tostring(maxr)}
```

`backoff_ms` 由客户端计算传入：`min(cap, base × 2^tries) + rand(0, base)`。**抖动不可省**——几千个任务同时失败时，无抖动的指数退避会形成同步重试风暴。把退避策略放在客户端而非脚本里，是"机制在服务端、策略在客户端"的分层：未来要换线性退避或按错误分类退避，不动热路径脚本。

DLQ 判据语义（勘误 #4，与黄金用例 T7 一致）：`MaxRetries` 是**首投之外**的额外重试上限，任务共投递 `max_retries+1` 次；`tries` 从 1 起计投递序号，`tries > max_retries` 才进 DLQ。与 sweep 路径的 `lease_resets > max_redeliveries` 同构（两者都数"首次之后的额外机会"）。

### 5.6 sweep（转移 #9/#10，租约回收——韧性核心）

```lua
-- KEYS[1]=lease KEYS[2]=ready KEYS[3]=sched KEYS[4]=dlq
-- ARGV[1]=task_key前缀 ARGV[2]=limit ARGV[3]=max_redeliveries ARGV[4]=retention_ms
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[2]))
local P48, requeued, dlqd = 281474976710656, 0, 0
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
      redis.call('HSET', tk, 'state', 'DLQ', 'last_error', 'lease_exceeded')
      redis.call('XADD', KEYS[4], '*', 'id', id,
        'payload', redis.call('HGET', tk, 'payload'), 'err', 'lease_exceeded',
        'tries', redis.call('HGET', tk, 'tries'), 'via', 'sweep', 'ts', now)
      redis.call('EXPIRE', tk, tonumber(ARGV[4]))
      dlqd = dlqd + 1
    else
      local pri = tonumber(redis.call('HGET', tk, 'pri'))
      redis.call('HSET', tk, 'state', 'READY', 'updated_ms', now)
      redis.call('ZADD', KEYS[2], pri * P48 + now, id)  -- 回到队尾（勘误 #5：ZPOPMIN 编码）
    end
  end
end
return {'SWEPT', tostring(requeued), tostring(dlqd)}
```

三条性质，全部来自"它是原子脚本 + 全量守卫"：

1. **幂等**：任何一步的前置检查不满足就走清理分支，多实例并发跑、重复跑，结果一致 → 无需选主，任何 worker 都能执行，回收器自身无单点。
2. **`ver+1` 是本方案唯一让旧 token 失效的地方**：sweep 重投的一瞬间，原 worker 手里的 token 变成废纸，它之后的 complete/heartbeat 全部 `ERR_FENCED`。这就是 Kleppmann fencing token 在 Redis 上的落地。
3. **`lease_resets` 上限是毒丸第二防线**：一个任务反复失败会走 fail→DLQ；但一个任务如果反复让 worker **崩溃/超时**（OOM 任务、反序列化炸弹），永远不会有人调 fail——必须有重投次数上限兜底，否则它在"超时→重投"里无限循环，且每次都杀死一个 worker。

### 5.7 promote（转移 #3）与 release（转移 #11）

promote：`ZRANGEBYSCORE sched -inf now LIMIT 0 N`，逐个校验 state==SCHEDULED，取原 score（visible_at，保证同优先级按到期序）换算成 ready score（`pri×2⁴⁸ + visible_at`，勘误 #5 编码）后迁移。与 sweep 同周期执行。

release：worker 优雅下线时调用，逻辑 = fail 的重试分支但 `tries` 不增加、`ver` 不 bump（fencing 纪律：ver 只在 sweep / fail-retry 失效，state 守卫已挡住幽灵写）、err 置 `released`，回 ready **队尾**（score=pri×2⁴⁸+now）。让正常重启的任务立刻可被领取，而不是白等一个 vis 周期。

## 6. 并发控制：竞态消灭矩阵

考核点正面回答——系统里每一个真实竞态，它的名字、若无保护的后果、以及被什么机制消灭：

| 竞态 | 若无保护 | 消灭机制 |
|---|---|---|
| 两个 worker 同时 reserve 同一任务 | 双重执行 | reserve 是单脚本，`ZPOPMIN` 原子弹出 |
| complete vs sweep（租约到期边界） | 已完成任务被重投，或完成丢失 | 同一脚本域内线性化，先到先赢；sweep 查 state，complete 摘除 lease 条目 |
| 慢 worker 租约易主后"复活"回来写 | 幽灵写覆盖新 owner 的状态 | fencing token：sweep `ver+1`，旧 token 一切操作 `ERR_FENCED` |
| fail 与 sweep 并发 | 双重重投、tries 错乱 | 脚本原子 + sweep 检查 state==RESERVED |
| complete 成功但响应丢失，客户端重试 | 重复执行副作用 | complete 幂等（`OK_DUP`） |
| 生产者重试 enqueue | 同一业务任务两份 | 业务 id 作任务 id，`EXISTS` 去重 |
| 多 sweeper 并发 | 重复 requeue | sweep 幂等（每步守卫），多跑无害 |
| Redis 主从切换丢尾部写入 | COMPLETED 回退 RESERVED → 重投 | 诚实地承认 at-least-once：消费侧幂等键兜底；fencing 挡住幽灵写 |

最后一行值得展开：**fencing token 能挡住"旧 owner 写新状态"，挡不住"旧主库的丢失写入"**（Sentinel 异步复制，failover 可能把一次 complete 永久丢掉，任务回退到 RESERVED）。对丢不起的场景，客户端对 complete 用 `WAIT 1 0` 确认已传播到至少一台 replica；终极兜底永远是消费侧幂等。

## 7. 消费侧幂等协议（effectively-once 的另一半）

队列只能负责"不丢"，"不重"的最后一步在消费侧，协议必须写进方案而不是留给约定：

1. 任务 payload 强制携带全局唯一 `task_id`（与队列 id 同源）；
2. worker 执行副作用前 `SET dedup:{task_id} <worker> NX EX 86400`——抢不到说明别人做过（或正在做），直接放弃；
3. 有数据库的场景改用事务性写法：副作用与 dedup 标记同事务提交（transactional outbox 同理）。

## 8. 韧性：故障矩阵

| 故障 | 系统行为 | 依赖的机制 |
|---|---|---|
| worker 领取后、处理前崩溃 | 租约到期 → sweep 重投 | vis + sweeper |
| worker 处理中崩溃（副作用已发生） | 重投 → 可能重复执行 | 消费侧幂等（第 7 节） |
| worker 未死但 GC 停顿/网络分区 | 心跳停 → 租约易主；复活后写被拒 | sweep + fencing |
| sweeper 所在 worker 挂了 | 无影响，其他 worker 的 sweep 继续跑 | sweep 幂等，无主化 |
| Redis 宕机 | AOF everysec 重放，最多丢 ~1s 尾部 | AOF + 主从 + 消费侧幂等 |
| 毒丸（反复 fail） | tries ≥ max → DLQ | fail 分岔 |
| 毒丸（反复杀死 worker，无人调 fail） | redeliveries 超限 → DLQ | sweep 的 lease_resets 上限 |
| 任务风暴（生产 > 消费） | 天然背压：reserve 是拉模式，worker 领不动就不领 | 拉模型 + 每 worker 并发上限 |

Redis 侧推荐配置：AOF `appendfsync everysec`（队列语义下不要 `always`，吞吐代价不成比例，尾部风险已由幂等覆盖）；Sentinel `down-after-milliseconds` 调小以缩短不可用窗口；接受 failover 窗口内的写入丢失并用 `WAIT`/幂等对冲——**不把 Redis 说成 CP 系统，是本方案诚实性的底线**。

## 9. 容量与性能预算

- 每任务生命周期 RTT：enqueue 1 + reserve 1（批量 N 摊薄到 ~1/N）+ heartbeat ⌈处理时长/(vis/3)⌉ + complete/fail 1。心跳是主要可变量：10 分钟任务、vis=30s ≈ 3 次。
- Redis 单节点简单 Lua 脚本 5–8 万 ops/s 量级 → 单队列节点支撑 **数千至数万任务/秒**；瓶颈先出现在 ZSET O(log N) 与网络 RTT，扩容路径 = 队列子分片。
- 内存：task hash ≈ payload + ~300B 元数据；ZSET entry ≈ ~100B。**100 万在途任务（1KB payload）≈ 1.5GB**。payload > 100KB 必须改存对象存储引用——队列里只放指针，这是纪律不是建议。
- sweeper 成本 O(到期任务数 × log N)，`limit` 参数（建议 200/轮）防止单轮长脚本阻塞 Redis。

## 10. 可观测性与运维

指标（全部可从上述结构直接推导）：`ready/sched/lease 深度`（ZCARD）、`oldest_ready_age`、`redelivery_rate`（sweep 返回值）、`dlq_rate`、`fence_reject_count`、`reserve 空转率`。

两条关键告警线：
- **oldest_ready_age 持续增长** → 消费能力不足或 worker 全灭；
- **fence_reject_count 突增** → vis 配小了，或 worker 频繁 GC/分区——这是唯一能在"用户感知到重复执行之前"发现租约误判的信号。

DLQ 运维：Stream 天然支持 `XRANGE` 检查、`XTRIM MAXLEN` 控制体积；replay = 读出快照，经 `replay.lua` 原子重放（state==DLQ 守卫 + 原子 DEL 残留 hash + 重新 enqueue，新 tries 周期；保留期内的残留 hash 需 `--force` 显式授权；在途任务一律拒绝），支持 dry-run。DLQ **必须有人消费的告警**，死信不是垃圾桶，是待办事项。

## 11. 边界情况清单

- **优先级饥饿**：严格优先级会让低档位饿死。限制档位数（≤3 档）+ 文档化"高优先级必须低量"的契约；需要严格公平就上 aging（score 随等待时间衰减），不要先上。
- **时钟**：一切到期判断用 Redis `TIME`。sweep 分轮执行，1–2s 的扫描周期误差是设计余量，不是缺陷——vis 不应该被配置得连 2s 余量都容不下。
- **保留期**：COMPLETED/DLQ 的 task hash 靠 EXPIRE 自动清理，retention 由队列表级参数控制；DLQ Stream 用 XTRIM 封顶。历史审计需求交给 Stream，不靠 hash 永生。
- **blocking reserve**：ZSET 没有阻塞弹出。拉模型 + 100–200ms 自适应轮询即可；要压低空转延迟，可用 LIST 只做唤醒信号（LPUSH 哨兵 + BLPOP），不承载任务数据。

---

**方案一页纸总结**：状态机上只有四个停留态（SCHEDULED/READY/RESERVED + 两个终态），全部转移收敛为 10 个 Lua 原子脚本（enqueue/reserve/heartbeat/complete/fail/release/sweep/promote/abandon/replay）；并发正确性靠"原子弹出 + fencing token（ver 字段）+ 脚本域线性化"三件套；超时靠 ZSET 租约索引 + 无主幂等 sweeper；毒丸靠两道上限（tries→fail 路径，lease_resets→sweep 路径）汇入 DLQ Stream；语义上诚实声明 at-least-once，用消费侧幂等协议收敛到 effectively-once。
