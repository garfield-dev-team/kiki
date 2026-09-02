# SDK 接入文档

> import `github.com/garfield-dev-team/kiki`（package `kiki`）。5 分钟上手请直接看 [`example/`](../example)；本文是完整接入参考。语义承诺见 README「语义承诺」节——不可协商。

## 0. 安装与前提

```bash
go get github.com/garfield-dev-team/kiki
```

- 依赖：Redis 5+（脚本按效果复制，`TIME` 安全）；生产推荐 7.2 + AOF everysec（[部署指南](operations.md)）。
- kiki 是库：你的进程就是 Producer/Worker，Redis 就是 broker。

## 1. 生产侧

```go
rdb := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{"127.0.0.1:6379"}})
q, err := kiki.NewQueue(kiki.QueueOptions{
    Redis: rdb,
    Name:  "emails", // 进入 hash tag {qk:emails}；字符集见 §6 约束
})
defer q.Close()

// 立即投递：id 是业务唯一键（幂等防线，重复投递得到 kiki.ErrDup，通常忽略）
err = q.Enqueue(ctx, "email:123:welcome", body,
    kiki.WithPriority(0),      // 0 最高；档位建议 ≤3（饥饿问题，design.md §11）
    kiki.WithMaxRetries(3),    // 首投之外的额外重试上限；0 = 队列默认 5
    kiki.WithHeaders(map[string]string{"traceparent": tp}),
)

// 延迟投递（SCHEDULED，到期由 promote 提升）
err = q.EnqueueIn(ctx, "email:456:digest", body, 30*time.Minute)

// 批量投递（单次流水线 RTT；任一校验失败整批拒绝，脚本级失败逐条报告）
err = q.EnqueueBulk(ctx, []kiki.Task{...})

// JSON 便捷构造
t, err := kiki.NewJSONTask("email:123:welcome", EmailJob{To: "a@b.c"})
```

**规则**：

- 任务 id 由生产者的业务唯一键充当（`^[A-Za-z0-9][A-Za-z0-9:_.-]{0,127}$`），队列不生成 id——它无法知道业务上的"同一个任务"是什么；
- payload ≤ 队列 `PayloadLimit`（默认 1 MiB）；**>100KB 改存对象存储引用**，队列里只放指针；
- 负优先级、负重试数会破坏状态机不变量，入队即报 `kiki.ErrInvalidArgument`。

## 2. 消费侧（Worker）

```go
w := q.NewWorker(kiki.WorkerOptions{
    Concurrency:       16,              // slot loop 数，默认 GOMAXPROCS
    VisibilityTimeout: 30 * time.Second, // 只能 ≤ 队列上限；禁止调大 vis 代替心跳
    HeartbeatInterval: 0,                // 默认 vis/3；kiki.HeartbeatDisabled 显式关闭（短任务）
    SchedulerInterval: time.Second,      // 首个 Worker 自动内嵌 Scheduler；0 = 不内嵌
    Middleware: []kiki.Middleware{
        middlewares.Timeout(30 * time.Second), // 处理超时 ≠ 租约超时
        middlewares.Dedup(rdb, 24*time.Hour),  // 消费侧幂等（§4）
        middlewares.Slog(slog.Default()),      // task_id/tries/ver 恒在字段里
        middlewares.Metrics(),                 // handler 直方图
    },
    Hooks: kiki.Hooks{
        OnDLQ:    func(t kiki.Task, via string, cause error) { /* 告警路由 */ },
        OnFenced: func(op string, t kiki.Task) { /* 租约易主 */ },
        OnPanic:  func(t kiki.Task, r any, stack []byte) { /* 崩溃采集 */ },
        OnHeartbeatLost: func(t kiki.Task, err error) { /* 连续 3 次心跳失败 */ },
    },
})
w.Handle(func(ctx context.Context, t kiki.Task) error {
    var job EmailJob
    if err := json.Unmarshal(t.Payload, &job); err != nil {
        return kiki.NonRetryable(err) // 坏消息不重试，直接 DLQ
    }
    if err := sendEmail(ctx, job); err != nil {
        if isQuotaExceeded(err) {
            return kiki.WithBackoff(err, 10*time.Minute) // 覆盖本轮退避
        }
        return err // nil → complete；普通 err → 指数退避重试
    }
    return nil
})

err = w.Run(ctx) // 阻塞；ctx 取消或 Shutdown 触发优雅下线
```

**handler 对 ctx 的义务**（godoc 同款约定）：handlerCtx 被取消意味着"租约可能已易主"。应在**下一个安全点**停止——即两次外部副作用之间；已发出的外部调用让它自然结束，不要回滚也不要重试。副作用是否落了一半，只有你自己的幂等逻辑知道（§4）。

## 3. 错误分类学（一律 `errors.Is`）

| 哨兵 | 何时收到 | 你该做什么 |
|---|---|---|
| `kiki.ErrDup` | enqueue/replay：id 已存在 | 幂等语义，通常忽略 |
| `kiki.ErrState` | 状态机不允许（stale 观察者） | 丢弃 |
| `kiki.ErrOwner` | 非租约持有者 | 丢弃 |
| `kiki.ErrFenced` | **token 已过期，租约已易主** | **停止一切后续动作并放弃**：不重试、不吞掉、不静默降级为成功 |
| `kiki.ErrGone` | 任务 hash 已过保留期 | 丢弃 |
| `kiki.ErrPayloadTooLarge` | 生产者侧超限 | 生产者修复 |
| `kiki.ErrInvalidID` / `ErrInvalidName` | 注入防线 | 修调用方 |
| `kiki.ErrClosed` | Queue 关闭后调用 | 收尾逻辑 |

失败包装器：`kiki.NonRetryable(err)` → abandon 直达 DLQ；`kiki.WithBackoff(err, d)` → 覆盖本轮退避；`kiki.PermanentOf(err) bool` 解包判断。

引擎级 API（`Reserve/Complete/Fail/Heartbeat/Release`）公开但只给特殊消费者用；**业务代码禁止绕过 Worker 手写状态迁移**——状态机完整性靠"只有一个写入者"维持。

## 4. 幂等：为什么必须有，以及怎么做

kiki 语义是 **at-least-once**：租约超时重投、Redis 主从切换丢尾、complete 响应丢失，都会产生重复投递。这不是缺陷是承诺——不声称 exactly-once。把重复收敛掉的最后一步在消费侧：

1. 简单场景：`middlewares.Dedup(rdb, ttl)`——副作用前 `SET kiki:dedup:<id> NX EX ttl`，抢不到说明别人做过，静默 ack；
2. 有数据库的场景：**副作用与 dedup 标记同事务提交**（transactional outbox 同理），比 Redis 键更硬；
3. 看到任务重复投递，先查 `kiki_fenced_total` 突增与 handler 长尾，再怀疑系统。

## 5. 生命周期与优雅下线

```
Run(ctx) 阻塞
  ├─ ctx 取消 或 Shutdown() 调用
  ├─ 1 draining：Terminator 切换语义
  ├─ 2 slot loop 停止 reserve
  ├─ 3 等待在途 handler 至 grace（默认 30s）
  ├─ 4 强制取消所有 handlerCtx
  └─ 5 被取消的任务以 Release 收尾（tries 不变、不计失败、立即可被接班 worker 领取）
Run 返回
```

- draining 期间 handler 自己返回的**真实错误仍走 Fail**（DB 超时不许伪装成下线）；
- `Shutdown` 幂等，返回非 nil 仅表示 grace 内未等完——下线流程仍完整；
- `Worker.Close() = Shutdown + Queue.Close`。

## 6. 配置速查

| 参数 | 默认 | 说明 |
|---|---|---|
| `QueueOptions.MaxRetries` | 5 | 首投之外的额外重试上限（毒丸防线之一，不做"0=关闭"） |
| `QueueOptions.MaxRedeliveries` | 20 | sweep 路径 lease_resets 上限（毒丸防线之二） |
| `QueueOptions.Retention` | 24h | 终态 task hash TTL |
| `QueueOptions.PayloadLimit` | 1 MiB | 超限 `ErrPayloadTooLarge` |
| `QueueOptions.DLQMaxLen` | 10000 | XTRIM MAXLEN ~ 封顶 |
| `QueueOptions.VisibilityTimeout` | 30s | vis 队列上限 |
| `WorkerOptions.HeartbeatInterval` | vis/3 | 0=默认；`HeartbeatDisabled` 关闭 |
| `WorkerOptions.PollInterval` | 100ms→400ms | 空转自适应退避 |
| `WorkerOptions.ShutdownGrace` | 30s | 下线在途等待 deadline |
| `WorkerOptions.Backoff` | 1s/60s/±0.5 | `ExponentialBackoff`；可注入自定义 `BackoffPolicy` |

全部指标与告警线见 [docs/operations.md](operations.md)；运维命令见 `kikictl --help`。

## 7. 并发安全与独立部署

**并发安全**：`NewQueue` 构造后的 `Queue` 可被任意多 goroutine 并发使用——生产 goroutine、消费 goroutine、多个 `Worker` 实例共享同一个 `Queue` 都没问题。可变共享状态只有两个原子标志（closed / scheduler 标记），脚本表与指标实现构造后不可变，底层经 go-redis 连接池（官方保证 goroutine-safe）执行；`-race` 下的黄金用例（T1 32 goroutine 并发 Reserve、T9 shutdown 并发等）持续覆盖。`Worker` 本身只做一件并发敏感的事：`Handle` 必须先于 `Run`（Run 之后再改 handler 行为未定义）。

**生产者与消费者独立部署**：完全可行，这正是设计形态（design.md §2——kiki 是库，Producer 与 Worker 是两个角色 import 同一库直连 Redis，没有进程耦合）。唯一契约是**队列名 + payload 编码**（建议 payload 用版本化 JSON）。实操要点：

| 事项 | 建议 |
|---|---|
| 消费者扩容 | 无状态水平扩：拉模型天然背压，worker id（host:pid:seq）天然唯一，无需选主、无需预注册 |
| Scheduler 归属 | 默认每个消费进程的首个 Worker 自动内嵌（幂等无主，多跑无害）；生产侧进程建议 `SchedulerInterval: 0` 关闭，避免无谓的 Redis 轮询 |
| **参数所有权** | 见下表——分服务后两侧的 `QueueOptions` 不再共享，配错即语义漂移 |
| 混部版本 | `kikictl version` 探测；脚本随库编译期冻结，新旧 worker 共存期间写路径互相兼容（§12 升级纪律） |
| 消费侧幂等 | 独立部署更显必要：`middlewares.Dedup` 或 DB 事务（§4），重复投递的最后防线在消费侧 |

**分服务部署时的参数所有权**（谁执行谁配置）：

| 参数 | 归属 | 漂移后果 |
|---|---|---|
| `MaxRetries` | **生产者**（入队时经 enqueue.lua 写死进 task hash） | 生产端调小 ⇒ 消费端看到的重试上限跟着变，DLQ 提前/延后 |
| `MaxRedeliveries` / `Retention` / `DLQMaxLen` | **跑 Scheduler 的进程**（sweep/trimDLQ 的调用方） | 只有保留期不一致时表现为 hash 过早过期（`ErrGone`） |
| `VisibilityTimeout`（队列上限） | **消费者**（vis 只在 reserve 时写入租约；钳制发生在消费进程自己的 Queue 上，不跨进程） | 无跨进程耦合，但建议两端按同一份 SRE 预算文档约定（心跳间隔 = vis/3 依赖它） |
| vis 下调 / 心跳 / 退避 / 并发 / grace | 消费者（WorkerOptions） | 仅影响本消费进程行为，风险低 |
| payload 上限 | 生产者（入队校验） | 消费端不校验 |

## 8. 单队列吞吐上限与 ShardedQueue（v0.2 预告）

队列名进入 hash tag `{qk:<name>}` ⇒ 该队列全部 key 绑定单一 cluster slot ⇒ **单队列吞吐封顶在单分片**（单机/Sentinel 下同理，上限是"单实例"）。加 Worker、加 Redis 节点都突破不了；不同队列名会自然散布到不同 slot。

- 判定：积压时先分清"消费能力不足"（`oldest_ready_age` 增长但 Redis 分片未饱和 → 加消费者有效）与"已达单队列上限"（分片 CPU/脚本执行饱和 → 加消费者无效）；
- 出路（v0.2 `ShardedQueue`，go-implementation.md §3.4）：逻辑队列拆 `name#0..N` 物理队列（不同 hash tag → 不同 slot），SDK 提供合并视图——按任务 id 哈希路由（同 id 恒落同分片）、跨分片联合 reserve、Stats/指标/Scheduler/DLQ 聚合。注意跨分片**没有严格全局 FIFO**（各分片独立弹出），将诚实写进文档；
- 量级参照：design.md §9（单分片 5–8 万 ops/s 上限）；实测见 [docs/benchmarks.md](benchmarks.md)。

## 9. FAQ

**Q：任务处理一半进程被 kill -9 会怎样？** 租约到期 → sweep 重投（`ver+1` 毒杀旧 token）。副作用可能已发生——这正是 §4 存在的原因。

**Q：为什么没有 FAILED 状态？** `fail` 是事件不是状态：失败要么带退避重排，要么进 DLQ。僵尸态只会骗人。

**Q：收到 ErrFenced 还能 complete 吗？** 不能，也不该：任何终结写都会被脚本拒绝。放下它，重投由 sweep 体系负责。

**Q：优先级会饿死低档位吗？** 严格优先级会。纪律：档位 ≤3、高优先级必须低量；需要严格公平再谈 aging（先不做）。
