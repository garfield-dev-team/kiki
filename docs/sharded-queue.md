# ShardedQueue 技术方案（v0.2）

> 状态：**方案定稿，未实施**。本文档是 v0.2 子分片能力的规范源头。
> 地位与裁决：状态机语义全部继承 design.md（v0.2 不改任何转移、不改任何脚本）；
> 本文档只规范"逻辑队列 → N 物理队列"的合成层。裁决顺序沿用 AGENTS §0：
> `scripts/` + integration > design.md > 本文档 > go-implementation.md。
> 实施时的三处同步义务照旧：本文档（规范）→ 复用 scripts/（实现，零改动）→ integration T14–T17 + 等价性矩阵（验证）。

## 0. 一句话

**ShardedQueue 不是新队列类型，是纯 SDK 合成层**：一个逻辑队列 `orders` 落成 N 个物理队列 `orders#0..N-1`（各自不同 hash tag → 不同 cluster slot），SDK 提供合并视图。零新脚本、零脚本改动、零状态机变更——v1 的全部原子性与竞态防线原封不动地在每个分片内生效。

## 1. 背景与动机

- v1 的队列名进入 hash tag `{qk:<name>}` ⇒ 该队列全部 key 绑定单一 cluster slot ⇒ **单队列吞吐封顶在单分片**（design.md §9：单分片 5–8 万 ops/s 上限；实测 docs/benchmarks.md：10 slot 并行 ≈8.8k reserve+complete 对/秒）。加 Worker、加 Redis 节点都突破不了。
- operations.md 容量决策树第三分支（"两者都饱和"）目前只有两条出路：过渡期按业务键前缀手动拆多个队列名（SDK 不感知，等于把合成分担给业务），或等 v0.2。
- agent-sandbox 场景（agent-sandbox-task-queue.md §四.1）的异构资源池（`qk:sbx:gpu:us-east`）是手动分片的实例——ShardedQueue 把这套手动操作产品化。

**判定前提**（扩容前必须先分清，见 operations.md）：积压但 Redis 分片未饱和 ⇒ 加消费者有效，不需要 ShardedQueue；分片 CPU/脚本执行饱和 ⇒ 唯一出路是子分片。

## 2. 目标与非目标

### 目标

1. 单逻辑队列吞吐随 N 近线性扩展，突破单 slot 上限；
2. 多生产者、多消费者**零协调**：无选主、无 assignment、无一致性哈希、无 rebalance 协议；
3. v1 语义逐分片完整保留（fencing / tries / ver / 双毒丸防线 / at-least-once），且用 T1–T13 等价性矩阵证明；
4. 运维面（Stats / DLQ / kikictl）对逻辑队列聚合呈现；
5. 纯增量演进：v1 用户不受任何影响，降级路径明确。

### 非目标（语义诚实，写进文档不写进代码）

| 非目标 | 原因 |
|---|---|
| 跨分片全局 FIFO | 各分片独立 ZPOPMIN；跨分片有序需要分布式协调，与零协调目标冲突 |
| 跨分片严格优先级 | 高优任务在 #3 分片抢不过 #0 分片在消费的低优任务；优先级只在分片内严格 |
| 跨分片原子性（含批量 enqueue） | Redis Lua 单脚本只能触达同 slot key——这是**架构级不存在**，不是实现取舍 |
| 跨分片一致性哈希 rebalance | N 是静态契约（§7），不存在动态分片集合 |
| 队列位次查询（`ZRANK` 排队位次） | 合并视图下只能给 N 次查询的近似合并，精确位次不再承诺 |

## 3. 架构总览

```
                      ┌─ {qk:orders#0} ── slot A ── ready/sched/lease/dlq/t:（v1 全套 key + 脚本）
生产者 ── route(id) ──┼─ {qk:orders#1} ── slot B ── 同上
（纯函数路由，         ├─ …                              每个 #k 就是一个标准 v1 Queue，
 无任何协调者）        └─ {qk:orders#N-1} ── slot …       拥有自己的 scheduler/sweep/DLQ

消费者 ── ReserveFor ── 乱序轮询/并行扇出 N 个分片，谁先 reserve 到算谁的（拉模型，零绑定）
```

- **实现形态**：`ShardedQueue` 内部持有 N 个标准 `*Queue`（`name = "orders#k"`），门面方法要么路由到单分片（Complete/Fail/Heartbeat/Release），要么扇出合并（EnqueueBulk/ReserveFor/Stats/ListDLQ）。N 个子队列共享一个 go-redis UniversalClient（Cluster 模式下由它按 key 自动路由 slot，无需为每个分片建连接）。
- **红线自查**（对照 AGENTS §2）：
  - 硬规则 1（唯一写入者）：✅ 全部写路径仍收敛到既有 10 个脚本；ShardedQueue 只选择"对哪个 key 布局调脚本"，无任何内联 eval、无绕过；
  - 硬规则 2（不改状态机）：✅ 零脚本改动、零转移变更；每分片独立跑 sweep/promote，无主幂等性（v1 红利）直接复用——**消灭选主的设计在这里第二次兑现**；
  - 硬规则 6（key 布局）：✅ 子队列 key 布局与 v1 完全同构（`{qk:orders#k}:ready` 等）；dedup 键依旧无 tag（跨分片幂等防线不随路由漂移——v1 的故意设计在此兑现）；
  - 硬规则 7（依赖冻结）：✅ 唯一新增引用是标准库 `hash/fnv`，无第三方依赖、go.mod 不动。

## 4. 命名与 key 布局

- 子队列名：`fmt.Sprintf("%s#%d", logical, k)`，k ∈ [0, N)。hash tag 为 `{qk:orders#k}`，N 个 tag → N 个不同 slot，由 Redis Cluster 自然散布到各主节点。
- **v1 已预留 `#`**：task.go 的 `queueRe = ^[A-Za-z0-9][A-Za-z0-9:_.#-]{0,127}$` 从第一天起允许 `#` 并注释"保留给子分片命名约定"（key 注入防线注释，§2.5）。v0.2 兑现该预留。
- 新校验规则：
  - `NewShardedQueue` 拒绝含 `#` 的逻辑队列名（`ErrInvalidName`）——逻辑名与物理名必须无歧义互斥；
  - 单队列 `NewQueue` 对含 `#` 的名字打 warn 日志（不拒绝，保持 v1 兼容）：该名字与 ShardedQueue 的物理队列命名空间重叠，建议改名；
  - `manifest`（§7）按逻辑名记录 N，进一步消除"裸物理队列被误当逻辑队列"的歧义。
- task id 校验不变（`idRe` 不含 `#`，id 保持不透明，**不**内嵌分片信息——决策记录见 §5.3）。

## 5. 路由设计

### 5.1 默认路由：`fnv1a64(task_id) mod N`

```go
func ShardOf(key string, n int) int { return int(fnv1a64(key) % uint64(n)) }
```

- 生产者侧唯一需要"分布式决策"的地方就是一个**纯函数**：无状态、无协调者，任何进程对同一 key 算出同一分片。
- 均衡性论证：task id 是调用方业务唯一键（v1 语义不变），实践中近似随机；FNV-1a 64 位对顺序 id（`order-1001`、`order-1002`）同样散布良好。若调用方 id 存在病态前缀聚集，用 `WithRouteKey`（§5.2）显式纠偏，而不是给路由加状态。
- **哈希函数冻结契约**：FNV-1a 64 一经发布不得更换（更换 = 全量任务重路由，破坏 RouteKey 保序与迁移假设）。写入本文档即冻结，实施时在 go-implementation.md §0 依赖策略同步记录。

### 5.2 可选路由：`WithRouteKey`（按业务键保序）

```go
q.Enqueue(ctx, "order-1001", payload, kiki.WithRouteKey("tenant-42"))
```

- 语义：路由键 = `tenant-42` 而非 task id ⇒ 同一租户的任务恒落同一分片 ⇒ **分片内严格 FIFO + 分片内严格优先级对同租户成立**（Kafka 按 key 分区的同款权衡）。
- 代价：业务键聚集会造成分片倾斜——路由键的均匀性责任转移给调用方。文档明示：路由键应选择高基数字段（租户 id、用户 id），不要选低基数字段（租户套餐、地区）。
- 在单队列 `*Queue` 上该选项被接受但忽略（同一调用代码可无缝在两种队列间迁移）。
- 高级用法（对应 agent-sandbox §四.1）：`WithRouteKey(userID)` 即可实现"每用户分片亲和"，作为单用户公平性的粗粒度手段；细粒度公平仍靠准入信号量（沙箱文档 §四.4）。

### 5.3 分片句柄：`Task.Shard` 字段（决策记录）

`Task` 增加 SDK 填充的业务只读字段：

```go
type Task struct {
    // ... v1 字段不变 ...
    // Shard 是任务所在分片下标（SDK 填充，业务只读；单队列恒为 -1）。
    // complete/fail/heartbeat/release 经由它路由回原分片。它是路由句柄，
    // 不是 id 的组成部分——id 保持不透明。
    Shard int
}
```

**考虑过并否决的替代方案：把分片号内嵌进 task id**（如 `sq3-9f2c…`）。否决理由：
1. id 由调用方提供（`Enqueue(ctx, id, …)`），改格式破坏 v1 兼容与业务语义（id 是业务唯一键，不是 SDK 内部结构）；
2. `idRe` 不允许 `#`，任何内嵌格式都要改校验面，收益不成比例；
3. `Task.Shard` 已覆盖全部正确性路径（见 §6.3），id 内嵌只剩"从日志行反查分片"这一调试便利，用 Stats 分片明细（§8）替代即可。

新哨兵错误：`ErrNoShard`——`Shard` 越界（如 Task 经跨版本序列化丢失句柄、消费进程 N 配置小于任务所在分片且手工构造 Task）时，终结写返回该错误。**调用方按 `ErrFenced` 同级纪律处理：不重试、不吞**（租约语境下它同样是"这单不归你管"）。

## 6. 生产侧

### 6.1 多生产者：零协调

- 多个生产者进程各自构造 `NewShardedQueue`（同名同 N），路由纯函数保证确定性收敛——**这就是全部的负载均衡机制**。没有需要解决的"多生产者分布式问题"，只有需要遵守的静态契约（同名同 N，由 manifest 校验兜底，§7）。
- 一个生产者进程内部：`ShardedQueue` 构造后不可变（同 v1 `Queue` 的原子 flag 模式），并发 enqueue 安全；go-redis UniversalClient 自身 goroutine 安全。

### 6.2 Enqueue / EnqueueIn / EnqueueBulk 语义

| API | 分片行为 | 语义 |
|---|---|---|
| `Enqueue` / `EnqueueIn` | 路由到单分片，调一次 enqueue.lua | 与 v1 完全一致（原子、fencing 不涉及） |
| `EnqueueBulk` | **按 task 逐个路由**，同一分片的任务合并成一次 pipeline | 批量**非原子**：跨分片无 all-or-nothing（v1 单队列内批量本就是逐条 pipeline，语义不劣化；诚实条款：需要批量事务语义的用例本队列不支持） |

- 失败模型与 v1 相同：单条 enqueue 失败即该条失败，调用方重试时同 id 幂等（enqueue.lua 的 NX 语义）。
- `WithRouteKey` 只影响路由，不影响 payload/headers。

### 6.3 终结写路由：为什么没有 misroute 问题

`complete/fail/heartbeat/release` 按 `Task.Shard` 路由。正确性链条：

```
ReserveFor 在 #k 上 reserve → 返回的 Task 已带 Shard=k
  → 该 Task 的全部终结写都路由回 #k → owner+token+ver 校验发生在同一个 key 布局上
```

任务全生命周期锁死在单一 hash tag 内，fencing/tries/ver 语义**逐字成立**。消费者进程 N 配置错误的后果被结构性限制为"轮询范围少几个分片"（少消费，绝不会写错分片——写路径由句柄路由，与本地 N 无关）。这是 §5.3 决策的直接收益。

`ErrFenced` 纪律不变（AGENTS 硬规则 8）：指标 + warn + 放弃。

## 7. 消费侧

### 7.1 多消费者：零绑定的拉模型

- **消费者与分片之间不存在任何绑定关系需要建立**——不需要 assignment、不需要一致性哈希、不需要 rebalance。每个消费者对 N 个分片轮流 reserve，ZPOPMIN 原子性保证"谁先到谁得"。
- 并发安全 = 分片内原子性（reserve.lua 在单 slot 内原子），这正是 T1/T2 黄金用例（32 goroutine 抢占无重复）验证过的场景；跨分片之间根本不需要原子性。
- 扩容：多起进程立即生效（对比 Kafka consumer group 的 rebalance 停顿）；缩容/崩溃：在途任务由租约 sweep 兜底。worker id（host:pid:seq）天然唯一，无预注册。
- 一致性哈希在**推模型**（服务端分配分区给消费者）才有意义；kiki 消费者自己来拉，这整块复杂度结构性不存在。

### 7.2 `ReserveFor` 合并视图：乱序轮询（默认）与扇出（可选）

```go
// ReserveFor(max=1) 的默认路径：每次调用重新洗牌分片顺序，顺序探测，
// 命中即返回。max>1 时按序填充直至 max 或全空。
order := rand.Perm(n)
for _, k := range order {
    ts, err := q.shards[k].ReserveFor(ctx, worker, vis, max-len(out))
    // 命中：给每个 Task 标 Shard=k，追加；max 满即返回
}
```

| 策略 | 空队列成本 | 空队列发现延迟 | 适用 |
|---|---|---|---|
| 乱序轮询（默认） | 每次空轮询 N 次 eval | 最坏 N×RTT + PollInterval | 默认：RTT（亚毫秒）远小于 PollInterval，尾延迟可忽略 |
| `ReserveFanout: true` | 同样 N 次 eval（并行） | ≈1×RTT | 大 N + 对投递延迟极敏感的场景 |

- 空转轮询税公式（运维容量输入）：`空闲 eval/s ≈ W × N / PollInterval`（PollInterval 空转自适应退避至 400ms 封顶，v1 机制沿用）。例：W=10 worker、N=8、400ms ⇒ ~200 eval/s/实例，对 Redis 可忽略；但 W×N 上千时必须调大 PollInterval。**这是 ShardedQueue 最实际的规模化成本**，写进 operations.md 容量节。
- 积压场景下轮询在首个命中分片即停，每轮重新洗牌 ⇒ 多消费者对分片的抢占长期均匀。
- **跨分片优先级不保证**（§2 非目标）：轮询命中顺序与优先级无关；即便 Fanout 模式也只取首个命中。分片内优先级严格（v1 语义）。

### 7.3 Worker 集成：零公共 API 破坏

v1 的 `Worker` 已依赖内部接口 `queueAPI`（ReserveFor/Complete/Fail/abandon/Heartbeat/Release），`*Queue` 满足之。v0.2：

1. `ShardedQueue` 实现同一接口（路由语义见 §6.3/§7.2）；
2. `Worker.q` 字段类型 `*Queue` → `queueAPI`（私有字段，非破坏）；
3. 新增 `func (sq *ShardedQueue) NewWorker(opts WorkerOptions) *Worker`，与 `(*Queue).NewWorker` 并存。

Terminator、心跳 keeper、slot loop、优雅下线、中间件、Hooks **零改动**——它们只依赖 Task 与接口。消费侧唯一新感知是 Task 多了只读 `Shard` 字段。

### 7.4 参数所有权（继承 sdk.md §7，分片维度补充）

| 参数 | 归属 | 分片维度语义 |
|---|---|---|
| `MaxRetries`（队列默认） | 生产者（写死进 task hash） | 所有分片必须一致——由"同一个 `ShardedQueue` 构造出来的分片共享一套 QueueOptions"结构性保证 |
| `MaxRedeliveries` / `Retention` / `DLQMaxLen` | 跑 Scheduler 的进程 | **per-shard 生效**：DLQ 全局上限 = N × DLQMaxLen，告警阈值按此换算 |
| `VisibilityTimeout` / 心跳 / 退避 / 并发 / grace | 消费者 | 与分片无关（逐 Task 生效） |
| `N`（Shards） | **schema 级契约** | 见 §9：manifest 校验，拒绝漂移启动 |
| `SchedulerInterval: 0`（生产侧关闭清扫） | 生产者 | 对 N 个分片同时关闭 |

## 8. 清扫、死信与可观测性

### 8.1 Scheduler / sweep：每分片一份

- `ShardedQueue` 首个 Worker 内嵌时拉起 N 个分片级 scheduler（复用 v1 `acquireScheduler`，各自 1s+抖动）。无主幂等 ⇒ 多进程多分片重叠运行无害，v1 红利直接兑现。
- 没有跨分片 sweep 的必要：租约、sched、ready 全在分片内闭环。

### 8.2 DLQ

- 物理布局：每分片一条 Stream `{qk:orders#k}:dlq`；`DLQMaxLen`、`Retention` per-shard。
- `ListDLQ(ctx, count)`：扇出各分片 `XRANGE`，按 `TS` 降序合并取前 count；`DLQEntry` 增加 `Shard int` 字段（回放路由句柄，与 Task.Shard 同理）。
- `ReplayDLQ(ctx, entries, force)`：逐条按 `e.Shard` 路由回原分片执行 replay.lua（v1 语义原样）。混合分片的批量回放 = 多次单分片脚本调用，无跨分片原子性（回放是运维低频操作，不需要）。
- kikictl 扩展：`dlq ls/replay` 自动探测 manifest，对 ShardedQueue 输出 `shard` 列、支持 `--shard k` 过滤；`dlq replay` 不带 `--shard` 时按条目 Shard 路由（幂等，replay.lua 的 state==DLQ 守卫保证重复执行安全）。

### 8.3 Stats 与指标

```go
type ShardedStats struct {
    Stats            // 聚合：各 Depth = Σ分片；OldestReadyAge = max(分片)
    Shards []Stats   // 下标 = 分片号，容量诊断与"卡分片"检测的原始数据
}
```

- 指标：聚合序列沿用 v1（`queue` 标签 = 逻辑名）；每分片增加 `shard` 标签序列。基数上界 = 队列数 × N ≤ 64（§11.2），可控。
- **卡分片检测 = N 漂移的核心告警信号**（§9.3）：部分分片 ReadyDepth 持续增长且 `oldest_ready_age` 上升、其余分片排空 ⇒ 几乎必然是消费者 N 配置落后于 manifest（或某分片所在 Redis 节点故障未 failover）。写进 operations.md 告警规则。

## 9. N 的治理：schema 级静态契约（本方案最重的一节）

### 9.1 为什么 N 不能动态

- 路由 `hash(key) mod N` 的重映射面 = N 的函数：改 N ⇒ 绝大多数 key 换分片。在途任务不受影响（key 已落定在原分片 hash tag 里），但 **RouteKey 保序被打破**（同一路由键的新任务落到新分片，跨分片无序——与 Kafka 增加分区后 key 失序同构）。
- 因此 N 的变更不是运行时操作，是**迁移操作**；一致性哈希不解决问题（它只优化"节点动态增减时的映射稳定性"，而这里的约束是"任务生命周期必须锁死单分片"——任何重映射对存量任务都无意义，对保序键都有害）。

### 9.2 manifest 键：N 的事实源与漂移熔断

- key：`kiki:sqmanifest:<logical>`（**不带 hash tag**，cluster 自由分布——与 dedup 键同命名空间纪律、同可用性等级；v1 有先例，不是新发明）。
- value：`{"n":8,"ts_ms":...,"by":"host:pid"}`。
- `NewShardedQueue` 握手（默认 `ManifestCheck: Strict`）：
  - manifest 不存在 → `SET NX` 写入本进程的 N（多进程并发首次创建竞态无害：先到先得，后到者读到 mismatch 即失败退出，暴露配置分歧——这正是想要的 fail-fast）；
  - 存在且 N 一致 → 通过；
  - 存在且 N 不一致 → **拒绝启动**，错误信息包含 manifest 值、本地配置值与修复指引（`kikictl sq manifest set`）。
- 降级模式 `ManifestCheck: Warn`（迁移窗口期/弱网环境显式开启）：mismatch 只打 error 日志不拒绝启动——把"可用性优先于严格性"的决定显式交给运维，而不是默默容忍。
- manifest 自身的可用性 = 其所在 slot 主节点的可用性（与 dedup 键同等级）；Strict 模式下它成为**启动期**依赖（运行期不复查，进程持续可用性不受影响）。诚实写进文档。
- Close 不删 manifest（N 是持久事实，重启后仍需校验）。

### 9.3 扩 N runbook（唯一支持的常规变更方向）

```
1. kikictl sq manifest set orders --shards 8        # 4 → 8，manifest 先行
2. 滚动重启生产者（Shards=8）
   - 仍在跑的旧生产者（N=4）不受影响：继续写 #0..3，key 自洽，无丢失；
   - 新启动的生产者若仍配 4 → Strict 拒绝启动 → 被迫完成升级（fail-closed 的意义）
3. 滚动重启消费者（Shards=8）
   - 仍在跑的旧消费者继续消费 #0..3；#4..7 的积压由"卡分片告警"暴露
4. 验证：ShardedStats.Shards 各分片有流量；旧分片 ReadyDepth 排空到稳态
5. 观测一个最长任务周期（含 DLQ 回放）后，迁移完成
```

全程无停机、无数据迁移（存量 key 不动）、可随时暂停——单调扩 N 的迁移是安全的。

### 9.4 缩 N：默认禁止

- `kikictl sq manifest set orders --shards 4`（从 8 缩）时，kikictl 逐个检查被移除分片（#4..7）的四项深度（Ready/Sched/Lease/DLQ）全为 0 才放行（尽力而为检查，存在检查后写入的窗口）；非零需 `--force` 并显式接受"被移除分片上的任务成为孤儿（只能以裸 `*Queue` 形式访问排空）"。
- SDK 层面不提供缩容迁移（重写存量 key 需要跨分片搬数据，复杂度与收益不成比例）。文档写明：**容量规划应预留 N 上限余量，把缩 N 当作事故恢复手段而非常规操作**。

## 10. 容量与性能

### 10.1 量级推算

- 单分片上限：design.md §9，5–8 万 ops/s（脚本执行 + 单实例瓶颈）；实测基线 8.8k 对/秒（10 slot，docs/benchmarks.md）。
- ShardedQueue 理论吞吐 ≈ N × 单分片上限（各分片落在不同主节点时）；同一主节点上的多个 slot 共享该节点 CPU，扩展系数打折——**N 的收益以"分片散布到足够多的主节点"为前提**，容量规划时检查 `CLUSTER SLOTS` 分布。
- 消费能力匹配：总消费 slot 数应 ≈ 总目标吞吐 × 单对耗时；分片多而 slot 不足时先加消费者（便宜），分片少而饱和时才扩 N（schema 变更）。

### 10.2 N 的建议上界：64

约束不是哈希质量而是轮询税（§7.2 公式）与指标基数。N ≤ 64 时：空闲 eval 成本在常规 PollInterval 下可忽略，指标序列可控，manifest/运维面简单。超过 64 的场景（十万级 ops/s 单队列）先审视是否该拆业务队列（不同业务语义本就应该是不同队列）。

### 10.3 验收基准（实施时对照 docs/benchmarks.md 纪律）

- 4-master compose（T12 环境），N=4，40 消费 slot：目标 ≥ 3 × v1 单分片对吞吐（≥ ~26k 对/s）；达不到须书面分析（预期瓶颈：单客户端连接的 slot 路由开销、跨节点 RTT 方差）；
- 空转 eval 成本实测 ≤ §7.2 公式值 × 1.5；
- 回退红线沿用：较基线回退 >20% 必须书面说明或回退改动（AGENTS §6）。

## 11. 集成测试计划（实施时三处同步的"验证"一环）

| 用例 | 内容 | 对应考核点 |
|---|---|---|
| **T14 路由与分布** | 路由确定性（同 id 恒同分片）；10k 任务 × N=4 分布均匀性（各分片 2500±20%）；`WithRouteKey` 同键恒同分片 | 路由冻结契约 |
| **T15 跨分片并发恰好一次** | 32 消费者 × N=4 抢 1k 任务：零重复、零丢失（T1/T2 的分片化扩展）；`-race` + `-count=10` | 零绑定拉模型 |
| **T16 N 治理** | 扩 N：manifest bump → 旧 N 消费者 Strict 拒启 → 新任务落新分片 → 全分片排空；缩 N 非空拒绝；Warn 降级模式行为 | schema 契约 |
| **T17 运维面聚合** | Stats 合并正确性；ListDLQ 跨分片合并与 Shard 标注；ReplayDLQ 按 Shard 路由回原分片回放成功 | 聚合视图 |
| **等价性矩阵** | **T1–T13 参数化跑在 `ShardedQueue(N=4)` 上**——v1 全部黄金用例逐字通过 ⇒ 语义等价的形式化证明 | 红线自查 |

全部用例跑在 4-master compose 集群（testcontainers redis:7.2 + `${KIKI_CLUSTER_ANNOUNCE_IP}`），脚本改动为零因此无新勘误，但 flaky 门禁照跑：`go test -race -count=10 ./integration/`。

## 12. 兼容性与升级

- **纯增量**：v1 用户不 import 新符号则零影响；`scripts/`、key 布局（单队列部分）、错误哨兵（只增不改）全部冻结。
- 唯一软建议：v1 队列名避免 `#`（当前合法但与物理命名空间重叠，NewQueue 会 warn）。
- 依赖：`hash/fnv`（标准库），go.mod 不动——AGENTS 硬规则 7 天然满足，实施时在 go-implementation.md §0 记录一句即可。
- **降级路径**：停用 ShardedQueue = 停止新 enqueue → 排空各分片（或接受孤儿任务用裸 `*Queue` 访问）→ 切回单队列。无脚本、无数据格式变更，降级与升级同样平凡。
- 版本探测：脚本随库编译期冻结（v1 机制），ShardedQueue 无脚本 ⇒ 不存在新旧 worker 脚本兼容问题；manifest 是唯一新共享状态，旧版本 SDK 不认识它、不受影响。

## 13. 诚实差距清单（v0.2 仍然解决不了的）

1. **跨分片原子性在 Redis Cluster 上架构级不存在**（单脚本单 slot）——本方案的全部设计都以"不需要跨分片原子"为前提倒推：路由纯函数、句柄路由、每分片闭环。任何"顺手加个跨分片事务"的提案都应按 AGENTS §3 的精神质疑。
2. 全局 FIFO / 全局严格优先级 / 精确排队位次：见 §2 非目标，v0.2 不提供，也无提供计划。
3. `EnqueueBulk` 跨分片非原子：单队列内也只是 pipeline 而非事务，语义未劣化，但批量事务需求方应另寻方案。
4. N 缩容无迁移：孤儿任务只能裸 `*Queue` 访问（§9.4）。
5. manifest 键的单点可用性（启动期，Strict 模式）：主节点故障 failover 期间新建连接可能失败——运行中进程不受影响。
6. RouteKey 保序在 N 变更时断裂（§9.1）：保序敏感业务要么接受、要么在 N 规划时预留足量余量避免变更、要么自建业务层序号。

## 14. 开放问题（实施前需拍板，不阻塞本方案定稿)

1. `ReserveFanout` 是否需要自适应模式（空队列轮询、积压时自动扇出）？——当前判断：YAGNI，PollInterval 自适应已够。
2. manifest 的 `Warn` 模式是否应该同时关闭"kikictl 对物理队列的直接操作警告"？——倾向否，警告独立。
3. 是否提供 `EnqueueShard(k)` 逃生门（调用方自选分片）？——倾向不提供：等于把路由责任交回调用方，破坏"纯函数路由"的单一事实源；有 RouteKey 已覆盖定向需求。
4. ShardedStats 的 `Shards []Stats` 是否合并进 v1 `Stats` 接口（`Stats` 保持、新增方法）还是平行类型？——倾向平行类型，避免 v1 类型膨胀。

## 15. 关联文档

- design.md —— 状态机与脚本规范（v0.2 继承，不改动）
- go-implementation.md §3.4 —— Cluster 注意事项与子分片命名约定的最初出处
- docs/sdk.md §7/§8 —— 并发安全、独立部署参数所有权、单队列吞吐上限（用户视角）
- docs/operations.md —— 容量决策树、告警规则（实施时补"卡分片检测"与 manifest runbook）
- docs/benchmarks.md —— 实测基线与验收纪律
- agent-sandbox-task-queue.md —— 资源池分片的落地场景（异构池 = 手动分片的实例）
