# Go 实施方案：kiki（任务队列中间件 + Client SDK）

> 命名出处：《魔女宅急便》——worker 是骑扫帚送件的 Kiki，任务是要送的件，DLQ 是送不到的件。
>
> 上游文档：`design.md`（状态机、脚本规范、竞态与韧性设计）。本文档负责"怎么在 Go 里把它造出来"：工程结构、并发模型、错误分类学、脚本加载、测试策略与实施里程碑。
>
> **勘误（相对 design.md §5）**：见本文 §3.5。`scripts/` 目录下的 Lua 为唯一规范版本，design.md 中的脚本为示意。

---

## 0. 定位与三个架构决策

1. **库，不是服务。** Redis 是 broker（design.md §2 的结论不变），Go 库是引擎。没有独立的"队列服务进程"需要部署——Producer 与 Worker 各自 import 本库，直连 Redis。好处：少一个跳数、少一个 SPOF、少一套部署；代价：协议兼容性由库的版本纪律保证（§12）。
2. **一个 module，两个门面，一个引擎。**
   - **引擎（middleware 层）**：`Queue` + `scripts/*.lua`——所有状态转移的唯一实现；
   - **SDK 门面**：`Producer`（生产侧 API）与 `Worker`（消费侧运行时）。
   两者同 module 不同包边界，但 API 上严格分层：业务代码只允许碰门面，禁止绕过门面直调引擎写状态——状态机的完整性靠"只有一个写入者"维持。
3. **脚本是规范，Go 是运输层。** Lua 脚本以 `go:embed` 编译进二进制，版本随库走（§12）。Go 代码里不允许出现任何内联 `eval` 字符串——防止"实现漂移出规范"。

**依赖策略**：必选 `github.com/redis/go-redis/v9`；可选 `prometheus/client_golang`（无注册器时全部指标为 noop）；日志走标准库 `log/slog`。**零框架**（不用 asynq/river/machinery 包着改——本方案的核心价值在脚本与并发模型，包壳只会丢失控制权）。v0.2 ShardedQueue 新增引用仅标准库 `hash/fnv`（路由函数冻结契约，docs/sharded-queue.md §5.1），go.mod 无新依赖。

---

## 1. 工程结构

```
kiki/
├── go.mod                    # module github.com/garfield-dev-team/kiki; go 1.22+
├── queue.go                  # Queue：引擎门面。NewQueue/Stats/ReplayDLQ/Close
├── producer.go               # Enqueue / EnqueueIn / EnqueueBulk（流水线）
├── worker.go                 # Worker 运行时：slot loop、shutdown、进程内 in-flight 账本
├── heartbeat.go              # 租约保活 goroutine（per-task）
├── sweeper.go                # Scheduler 运行时：sweep + promote + XTRIM 循环
├── task.go                   # Task 结构、EnqueueOption、context carrier
├── errors.go                 # 哨兵错误 + Lua status → error 映射
├── backoff.go                # BackoffPolicy 接口与实现
├── middleware.go             # Middleware 链 + Terminator（终止型中间件）
├── middlewares/              # 内建中间件：recover.go / dedup.go / timeout.go / metrics.go
├── scripts/                  # ★ 10 个 .lua，规范所在，go:embed
│   ├── enqueue.lua  reserve.lua  heartbeat.lua  complete.lua
│   ├── fail.lua     release.lua sweep.lua      promote.lua
│   ├── abandon.lua           # 非 retryable 直接 DLQ（§6.2）
│   └── replay.lua            # DLQ 回放原子路径（§3.5 勘误 #6）
├── internal/rdb/             # UniversalClient 构造、key 布局、脚本加载与 warmup
├── internal/metrics/         # noop / prometheus 双实现
├── middlewares/              # 内建中间件：dedup.go / timeout.go / metrics.go / slog.go
│                             # （Recover 内联于 Terminator，§7）
├── cmd/kikictl/              # 运维 CLI（§11）
├── integration/              # testcontainers 黄金用例（§10）
└── docker-compose.test.yml   # redis 单机 + 4-master cluster 测试环境
```

---

## 2. 数据类型与公共 API

### 2.1 Task

```go
type Task struct {
    ID         string            // 生产者业务唯一键（§2.5 校验规则）
    Payload    []byte            // 不透明字节；SDK 提供 JSON 便捷构造
    Priority   int               // 0 最高；档位建议 ≤3（design.md §11 饥饿问题）
    MaxRetries int               // 0 → 取队列默认
    Delay      time.Duration     // >0 → SCHEDULED
    Headers    map[string]string // traceparent 等旁路元数据，hash 内存单字段 JSON

    // ---- 以下字段 reserve 后由 SDK 填充，业务只读 ----
    Owner         string        // 租约持有者（脚本 owner+token 双重校验，勘误 #8）
    Tries         int           // 第几次投递（从 1 起）
    Ver           int64         // ★ fencing token（design.md §4.3）
    LeaseDeadline time.Time     // 当前租约到期时刻（Redis 服务器时钟）
}
```

**Fencing 不变式**（勘误 #7）：第 k 次投递 `Ver = 2k−1`——reserve 颁发与 sweep 失效交替各 +1；release 不 bump ver。

**刻意不设 `Kind`/`Topic` 字段**：任务类型路由 = 队列名路由（一个业务类型一个 `Queue` 实例）。reserve 后按 Kind 过滤再 requeue 是伪装成功能的 requeue 风暴，不提供。

### 2.2 生产侧

```go
func (q *Queue) Enqueue(ctx context.Context, id string, payload []byte,
    opts ...EnqueueOption) error                       // ERR_DUP → ErrDup
func (q *Queue) EnqueueIn(ctx context.Context, id string, payload []byte,
    delay time.Duration, opts ...EnqueueOption) error  // 上式的 sugar
func (q *Queue) EnqueueBulk(ctx context.Context,
    tasks []Task) error                                // pipeline N 次脚本调用
```

便捷构造：`kiki.NewJSONTask(id, any) (Task, error)`。

### 2.3 消费侧（引擎级，Worker 运行时建立在它之上）

```go
func (q *Queue) Reserve(ctx context.Context, vis time.Duration, max int) ([]Task, error)
func (q *Queue) Complete(ctx context.Context, t Task, result string) error
func (q *Queue) Fail(ctx context.Context, t Task, cause error, backoff time.Duration) (FailResult, error)
func (q *Queue) Heartbeat(ctx context.Context, t Task, extend time.Duration) error
func (q *Queue) Release(ctx context.Context, t Task) error   // 优雅下线：tries 不变
```

`FailResult ∈ {Retried, DeadLettered}`，由脚本 `RETRY/DLQ` 状态映射——调用方据此决定是否告警。

### 2.4 错误分类学（SDK 的骨架）

所有脚本状态收敛为哨兵错误，业务侧一律 `errors.Is` 判断：

| Lua STATUS | Go 哨兵 | 语义 | 调用方应做 |
|---|---|---|---|
| `OK` / `SWEPT` | `nil` | 成功 | — |
| `OK_DUP` | `nil`（`metrics.result=dup`） | complete 幂等重试 | — |
| `RETRY` | `nil` + `FailResult=Retried` | 已按退避重排 | — |
| `DLQ` | `nil` + `FailResult=DeadLettered` | 已进死信 | 触发 OnDLQ hook |
| `ERR_DUP` | `ErrDup` | enqueue：id 已存在 | 幂等语义，通常忽略 |
| `ERR_STATE` | `ErrState` | 状态机不允许 | 丢弃（stale 观察者） |
| `ERR_OWNER` | `ErrOwner` | 非租约持有者 | 同上 |
| `ERR_FENCED` | `ErrFenced` | **token 已过期，租约易主** | 停止一切后续动作；副作用可能已被重做 |
| `ERR_GONE` | `ErrGone` | 任务 hash 已过保留期 | 丢弃 |
| `ERR_PAYLOAD` | `ErrPayloadTooLarge` | 超 payload 上限 | 生产者侧修复 |

失败包装器（供 handler 用，决定 fail 的走向）：

```go
kiki.NonRetryable(err)        // → abandon.lua，跳过剩余重试直接 DLQ
kiki.WithBackoff(err, d)      // → 本轮退避覆盖为 d
kiki.PermanentOf(err) bool    // 解包判断
```

```go
type FailResult int
const (Retried FailResult = iota; DeadLettered)
```

### 2.5 ID 校验（key 注入防线）

task_id 会成为 key 名的一部分（`{qk:q}:t:<id>`）与 ZSET member。enqueue 入口强校验：

```go
var idRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:_.-]{0,127}$`)
```

不匹配 → `ErrInvalidID`。这是动态拼 key（design.md §4.3）换来的纪律成本，在 API 边界一次付清。

### 2.6 Context carrier

```go
func TaskFromContext(ctx context.Context) (Task, bool)  // handler 内取任务元数据
```

---

## 3. Redis 执行层（internal/rdb）

### 3.1 客户端

统一 `redis.UniversalClient`，一个配置类型覆盖单机 / Sentinel / Cluster：

```go
redis.NewUniversalClient(&redis.UniversalOptions{
    Addrs: []string{"127.0.0.1:6379"}, // 或 MasterName: "mymaster"
})
```

SDK 内每操作叠加 `ReadTimeout=1s`（脚本都是毫秒级，1s 已含重试余量）；调用方 ctx 控制总预算。

### 3.2 脚本嵌入与加载

```go
//go:embed scripts/*.lua
var scriptFS embed.FS

var scripts = map[string]*redis.Script{ /* 逐文件 redis.NewScript(src) */ }
```

- `redis.Script.Run` 自带 EVALSHA→NOSCRIPT→EVAL 回退，正确性无忧；
- 仍保留 `Warmup(ctx)`：`NewQueue` 时对全部脚本 `script.Load`（主从/集群每节点一次），把首次调用的回退抖动挪到启动期；
- `Close` 无需注销脚本（flushall/重启才清，NOSCRIPT 回退兜底）。

### 3.3 Key 布局（代码即文档）

```go
func keys(name string) Keys {
    p := "{qk:" + name + "}"
    return Keys{Ready: p + ":ready", Sched: p + ":sched", Lease: p + ":lease",
        DLQ: p + ":dlq", TaskPrefix: p + ":t:"}
}
```

去重键独立命名空间、不带 hash tag（与队列无关，允许 cluster 自由分布）：`kiki:dedup:<task_id>`。

### 3.4 Cluster 注意事项（诚实条款）

- 所有脚本 KEYS 共享 hash tag `{qk:q}` → go-redis 按 KEYS[1] 路由，脚本内动态拼出的 task key 必然同 slot；
- **脚本经 ARGV 前缀动态访问 key** 在 Cluster 语义上属于"文档不背书、工程上成立"：Redis 官方要求 key 走 KEYS 声明以便路由，但 pop 前无法预知 id，动态 key 不可避免。同 slot hash tag 保证不会跨节点执行，BullMQ 在 Cluster 上即按此模式生产。我们将它写进文档并配 T12 集群冒烟用例，而不是假装它不存在；
- 热队列子分片：命名约定 `orders#0..N`，各成独立 slot，SDK 侧提供 `ShardedQueue` 合并视图（v0.2，非首版范围）。v0.2 完整技术方案已定稿：见 [docs/sharded-queue.md](docs/sharded-queue.md)（纯 SDK 合成、零脚本改动）。

### 3.5 勘误（design.md §5 脚本 → scripts/ 规范版）

> 本节 append-only：只增不改不删（AGENTS.md §0）。M1–M4 实施中新增如下。

1. **heartbeat.lua**：design.md 中 `ZADD lease dl ARGV[2]` 的 member 写错了（ARGV[2] 是 token，不是任务）。规范版为 `ZADD lease dl <task_id>`——lease ZSET 的 member 恒为 task_id，否则 sweep 永远扫不到被续期的任务。
2. **complete.lua**：design.md 中 `ZREM` 一行的 `ARGV[2] == ARGV[2] and tk and ...` 是示意伪码。规范版从 ARGV 传入 task_id，直接 `ZREM lease <task_id>`。
3. **新增 abandon.lua**（§6.2）：design.md 未覆盖"非 retryable 立即 DLQ"路径，fail 脚本只在进 DLQ 判据命中才死信，语义不闭合。守卫同 fail（state==RESERVED + owner + token），`via='abandon'`。
4. **DLQ 判据（fail.lua）**：design.md 原为 `tries >= max_retries`；规范版为 `tries > max_retries`——`MaxRetries` 语义 = 首投之外的额外重试上限，共 `max_retries+1` 次投递（黄金用例 T7 的构造即按此语义），与 sweep 路径 `lease_resets > max_redeliveries` 同构。design.md §5.5 与转移表 #7/#8 已同步。
5. **ready score 编码**：design.md 原 `pri×2⁴⁸ + (2⁴⁸−1−enqueue_ms)` 与 `ZPOPMIN` 组合是 **LIFO**，且 sweep「回到队尾」写 `(2⁴⁸−1−now)` 反而插队。自洽组合为 `ZPOPMIN + pri×2⁴⁸ + ts_ms`：pri 越小越优先、同优先级 FIFO、重投回队尾（score=pri×2⁴⁸+now）、promoted 任务带原 visible_at 保到期序。design.md §4.3/§5.1/§5.6 已同步。
6. **新增 replay.lua**：DLQ 回放若由运维侧裸 DEL+enqueue 实现，是 check-then-act 竞态。规范脚本内原子完成：仅 state==DLQ（终态）允许清残留 hash，`force=0` 时保留期内返回 `ERR_DUP`，在途任务一律拒绝（kikictl `dlq replay --force/--dry-run` 的后端）。
7. **T3 断言算术**：reserve 颁发 +1、sweep 失效 +1、再 reserve +1 ⇒ 一次重投后 `Ver=3` 而非 §10.1 原文写的 2。不变式：第 k 次投递 `Ver = 2k−1`（sweep 路径）/ release 路径因不 bump ver 而另计（T9）。
8. **owner 进 Task**：脚本以 owner+token 双重校验租约身份，reserve 返回的 worker id 必须随 Task 持有并回传；Task 结构补充 `Owner` 字段（§2.1）。
9. **零值即默认**：QueueOptions/WorkerOptions 的数值零值一律取 §9 默认（毒丸上限等防线不做"0=关闭"语义——关闭重试上限无法经零值表达是有意的）；WorkerOptions 的 `HeartbeatInterval` 例外：0=默认 vis/3，显式 `HeartbeatDisabled(-1)` 才关闭。
10. **panic 分类的落点**：Terminator 的 recover 必须包在 handler 调用处（panic 错误进入与普通错误相同的终结分类），不能只挂在 Terminator 函数体外——否则 panic 后 switch 不执行，任务既不 complete 也不 fail，靠租约到期兜底是浪费。

---

## 4. Worker 运行时（并发模型——本文档的心脏）

### 4.1 goroutine 拓扑

```
Worker.Run(ctx)
 ├── slotLoop × Concurrency（默认 GOMAXPROCS）
 │     └── reserve(1) ──► process(task)  [同 goroutine 内联执行]
 │            ├── handlerCtx := WithCancel(slotCtx)
 │            ├── startHeartbeat(slotCtx, task) → 失败时 cancel(handlerCtx)
 │            └── chain(Recover → 用户中间件 → Terminator)(handlerCtx, task)
 └── （可选）sweeper.Scheduler：本进程兼任时才启动
```

设计取向：**reserve 与处理同 goroutine 内联，Prefetch 默认 1**。批量预取（`Prefetch=N`）只摊薄 RTT，却把"崩溃未处理窗口"放大 N 倍、并引入批内并发记账——首版不启用，留作 `WithPrefetch(n)` 实验项并文档化此代价。

### 4.2 slot loop

```go
func (w *Worker) slotLoop(ctx context.Context) error {
    idle := w.opts.PollInterval // 100ms 起
    for {
        if ctx.Err() != nil { return ctx.Err() }
        tasks, err := w.q.Reserve(ctx, w.opts.Vis, 1)
        if err != nil {
            if ctx.Err() != nil { return ctx.Err() }
            w.log.Error("reserve failed", "err", err)
            w.sleep(ctx, idle); continue // 网络错误：退避轮询，不退出
        }
        if len(tasks) == 0 {
            idle = min(400*time.Millisecond, idle*2) // 自适应空转退避
            w.sleep(ctx, idle); continue
        }
        idle = w.opts.PollInterval
        w.process(ctx, tasks[0]) // 内联；wg 记账见 §4.5
    }
}
```

### 4.3 heartbeat keeper（租约保活 + 幽灵自我了断）

```go
func (w *Worker) startHeartbeat(slotCtx context.Context, t Task, fence func()) *sync.WaitGroup {
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        tk := time.NewTicker(w.opts.HeartbeatInterval) // 默认 vis/3
        defer tk.Stop()
        fails := 0
        for {
            select {
            case <-slotCtx.Done():
                return
            case <-tk.C:
                err := w.q.Heartbeat(slotCtx, t, w.opts.Vis)
                switch {
                case err == nil:
                    fails = 0
                case errors.Is(err, ErrFenced), errors.Is(err, ErrGone):
                    w.metrics.Fenced.Add(1)   // ★ 租约已易主
                    fence()                    // cancel handlerCtx（advisory abort）
                    return
                default:
                    fails++                    // 网络抖动：下一 tick 重试
                    if fails == 3 { w.hooks.OnHeartbeatLost(t, err) }
                }
            }
        }
    }()
    return &wg
}
```

关键语义：**heartbeat 失败只做两件事——取消 handlerCtx（建议性中断）与计数**。绝不代替 handler 去 complete/fail：租约已丢，任何终结操作都会被脚本拒绝；副作用是否落了一半，只有 handler 自己的幂等逻辑知道（design.md §7）。

**handler 对 ctx 的义务**（写进 godoc）：handlerCtx 被取消意味着"租约可能已易主"，handler 应在**下一个安全点**停止——即两次外部副作用之间；已经发出的外部调用让它自然结束，不要回滚也不要重试。

### 4.4 终结型中间件（Terminator）——complete/fail 的唯一入口

```go
func (w *Worker) terminator() Middleware {
    return func(next Handler) Handler {
        return func(ctx context.Context, t Task) (err error) {
            defer func() { // panic → 可重试失败（栈随任务存档）
                if r := recover(); r != nil {
                    w.hooks.OnPanic(t, r, debug.Stack())
                    err = fmt.Errorf("panic: %v", r)
                }
            }()
            herr := next(ctx, t)
            switch {
            case herr == nil:
                w.finish(ctx, t, complete)          // OK / OK_DUP
            case w.fenced(ctx, t):                   // heartbeat 已判 fence
                w.metrics.Fenced.Add(1)              // 丢弃：不再 complete/fail
            case isPermanent(herr):
                w.finish(ctx, t, abandon(herr))      // abandon.lua → 立即 DLQ
            case w.draining.Load() && ctx.Err() != nil:
                w.q.Release(context.WithoutCancel(ctx), t) // 下线路径：不计失败
            default:
                backoff := w.backoff.Next(t.Tries)   // 策略在客户端（design.md §5.5）
                if d, ok := backoffOf(herr); ok { backoff = d }
                w.q.Fail(context.WithoutCancel(ctx), t, herr, backoff)
            }
            return nil // 槽位永远继续
        }
    }
}
```

终结操作的结果处理矩阵（complete/fail 共用）：

| 脚本返回 | Worker 动作 |
|---|---|
| OK / RETRY / DLQ | 记指标；DLQ 另触发 `OnDLQ` hook |
| OK_DUP | 记指标 `result=dup`（响应丢失重试的自愈证据） |
| ERR_FENCED | **不重试、不报错**。`fenced_total++`，warn 日志。任务已易主，重投由 sweep 体系负责 |
| ERR_STATE / ERR_GONE | debug 日志（终态保留期后过期属正常），计数 |

`context.WithoutCancel(ctx)`：终结写必须穿过已取消的 handlerCtx 落盘——"把结果告诉队列"不能因为超时半途而废（半途 = complete 丢响应 = 重投，幂等能兜但没必要）。

### 4.5 优雅下线（`Worker.Shutdown(ctx)`）

顺序即正确性：

```go
func (w *Worker) Shutdown(ctx context.Context) error {
    w.draining.Store(true)          // 1. Terminator 切换语义
    w.cancelDispatch()              // 2. slot loop 停止 reserve（在途 reserve 自然结束）
    err := w.inflight.WaitCtx(ctx)  // 3. 等待在途 handler 至 grace deadline
    w.cancelHandlers()              // 4. 到点强制 cancel 所有 handlerCtx
    w.inflight.Wait()               // 5. Terminator 以 Release 收尾（tries 不变）
    return err
}
```

要点：
- **draining 期间的错误分类**：handler 自己返回真实错误 → 仍走 `Fail`（真实的失败必须计数）；仅"因 shutdown 被取消"的任务走 `Release`。判据是 `w.draining && ctx.Err() != nil`，二者缺一不可——否则 DB 超时会伪装成下线，任务无限 release 循环；
- Release 而非坐等租约到期：让重启后的接班 worker 立即可领取（design.md 转移 #11 的动机）；
- `Shutdown` 幂等，`Close = Shutdown(30s) + Queue.Close`。

### 4.6 in-flight 账本

`inflight` 为 `sync.WaitGroup` + 完成通道的组合，`WaitCtx` 支持受 ctx 约束的等待。每 process 开始 `Add(1)`、Terminator 返回后 `Done()`——账本只服务 shutdown，不参与正确性（正确性全在 Redis 侧状态机）。

---

## 5. Scheduler 运行时（sweep + promote，无主）

```go
func (s *Scheduler) Run(ctx context.Context) {
    for {
        interval := s.opts.Interval + rand.Duration(0, s.opts.Interval/5) // 抖动防齐步
        select {
        case <-ctx.Done(): return
        case <-time.After(interval):
            s.q.sweep(ctx, s.opts.Limit, s.opts.MaxRedeliveries)  // 200/轮
            s.q.promote(ctx, s.opts.Limit)                        // sched → ready
            s.q.trimDLQ(ctx)                                      // XTRIM MAXLEN ~ cap
            s.gaugeOldestReady(ctx)                               // oldest_ready_age 采集
        }
    }
}
```

- **谁跑**：默认 `Worker` 首个实例自动内嵌一个 Scheduler；`WithSchedulerInterval(0)` 关闭后可用 sidecar/独立进程跑（`kikictl sweep`）。无主幂等（design.md §5.6），多跑无害，故不做选主；
- **不自愈的代价要说清**：若所有进程都关掉了 Scheduler 且无人接手，SCHEDULED/超时租约会滞留——这是配置错误，靠 `oldest_ready_age` 与 `lease_depth` 告警暴露，而不是靠造一个主节点来掩盖。

---

## 6. 退避与错误分类策略

### 6.1 BackoffPolicy

```go
type BackoffPolicy interface {
    // tries: 已尝试次数（1 起）。由 SDK 调用方传入 rand 源，可注入。
    Next(tries int, rng *rand.Rand) time.Duration
}

func ExponentialBackoff(base, cap time.Duration, jitter float64) BackoffPolicy
// 默认 base=1s cap=60s jitter=0.5：min(cap, base·2^(tries-1)) · (1+U(−j,j))
```

策略在客户端、机制在脚本（design.md §5.5 的分层原则）：SDK 算好 `backoff_ms` 传给 fail.lua。

### 6.2 abandon.lua（非 retryable 立即死信）

design.md 的 fail 分岔只在 `tries ≥ max_retries` 时进 DLQ；`NonRetryable`（如参数非法、4xx 类错误）重试纯属浪费。新增脚本：守卫同 fail（state==RESERVED + owner + token），命中后直接走 DLQ 分支（XADD 快照、ZREM lease、EXPIRE、state=DLQ），`via='abandon'`。

---

## 7. 中间件链

```go
type Handler func(ctx context.Context, t Task) error
type Middleware func(Handler) Handler

func Chain(mws ...Middleware) Middleware // 从外到内依序包裹
```

装配顺序固定为：`[Terminator(最外层，SDK 私有), Recover, 用户中间件...]`。Recover 在 Terminator 内联实现（§4.4 defer），用户中间件永远拿到"不会 panic 穿透"的保证，也永远无法拦截终结语义（它们之下没有比 complete/fail 更低的一层）。

### 7.1 内建中间件

| 名称 | 行为 | 备注 |
|---|---|---|
| `middlewares.Dedup(rdb, ttl)` | 副作用前 `SET kiki:dedup:<id> NX EX ttl`，抢不到 → 静默 ack | design.md §7 协议的 SDK 化；Redis 不可用时返回 err（当失败重试，保守正确） |
| `middlewares.Timeout(d)` | `context.WithTimeout` 包 handler | 处理超时 ≠ 租约超时；前者触发 Fail，后者触发重投 |
| `middlewares.Metrics()` | handler 直方图 + 结果计数 | |
| `middlewares.Slog(logger)` | 结构化日志（task_id/tries/ver 恒在字段里） | |

**去重键无 hash tag**（§3.3）：它不属于任何队列的状态机，cluster 自由分布即可。

---

## 8. 可观测性

### 8.1 Prometheus 指标（internal/metrics，未注册时 noop）

| 指标 | 类型 | 标签 | 来源 |
|---|---|---|---|
| `kiki_ready_depth` | Gauge | queue | ZCARD ready |
| `kiki_sched_depth` | Gauge | queue | ZCARD sched |
| `kiki_lease_depth` | Gauge | queue | ZCARD lease |
| `kiki_oldest_ready_seconds` | Gauge | queue | ZRANGE WITHSCORES 头元素换算 |
| `kiki_enqueue_total` | Counter | queue, result(ok/dup/err) | producer |
| `kiki_reserve_total` | Counter | queue, result | worker |
| `kiki_complete_total` | Counter | queue, result(ok/dup/fenced) | Terminator |
| `kiki_fail_total` | Counter | queue, result(retry/dlq/fenced) | Terminator |
| `kiki_fenced_total` | Counter | queue, op(hb/complete/fail) | ★ 告警线 |
| `kiki_dlq_total` | Counter | queue, via(fail/sweep/abandon) | |
| `kiki_sweep_requeued_total` | Counter | queue | scheduler |
| `kiki_handler_duration_seconds` | Histogram | queue | middleware |

### 8.2 告警基线（承接 design.md §10）

- `kiki_oldest_ready_seconds` 持续增长 → 消费能力不足 / Scheduler 失联；
- `rate(kiki_fenced_total[5m])` 突增 → vis 过小或 worker GC/分区——重复执行的前哨；
- `rate(kiki_dlq_total[5m]) > 0` 持续 → 死信必须有人处理。

### 8.3 Hooks

```go
type Hooks struct {
    OnFenced        func(op string, t Task)
    OnDLQ           func(t Task, via string, cause error)
    OnPanic         func(t Task, r any, stack []byte)
    OnHeartbeatLost func(t Task, err error)
}
```

指标回答"多少"，hooks 回答"哪个"——接告警路由与采样日志。

---

## 9. 配置默认值与调参表

| 参数 | 默认值 | 约束/说明 |
|---|---|---|
| `VisibilityTimeout` | 30s | 队列上限；worker 可下调不可上调（上调被钳制并告警）。禁止用调大 vis 代替心跳 |
| `HeartbeatInterval` | vis/3 | 0 = 取默认；`HeartbeatDisabled(-1)` 才关闭（短任务场景，勘误 #9） |
| `MaxRetries`（队列默认） | 5 | per-task `WithMaxRetries` 覆盖；首投之外的额外重试上限（勘误 #4）。**零值 = 取默认**，毒丸防线不做"0=关闭"（勘误 #9） |
| `MaxRedeliveries` | 20 | lease_resets 上限（sweep 路径毒丸防线） |
| `Backoff` | 1s/60s/jitter 0.5 | ExponentialBackoff |
| `SchedulerInterval` | 1s（+20% 抖动） | 0 = 本进程不跑；≈2s 的扫描误差是设计余量 |
| `SweepLimit` | 200/轮 | 防长脚本阻塞 Redis |
| `Retention` | 24h | 终态 task hash TTL |
| `DLQMaxLen` | 10 000 | XTRIM MAXLEN ~（近似裁剪） |
| `PayloadLimit` | 1 MiB | 超限 `ErrPayloadTooLarge`；>100KB 建议对象存储指针（design.md §9） |
| `PollInterval` | 100ms→400ms 自适应 | 空转退避 |
| `ShutdownGrace` | 30s | Shutdown 默认 deadline |

---

## 10. 测试策略

**分层铁律：脚本正确性只在真实 Redis 上验证。** miniredis 的 Lua 子集（gopher-lua）不覆盖 `TIME`/效果复制等语义，仅用于 backoff/middleware/shutdown 的纯 Go 单测。脚本与运行时的真相在 testcontainers（`redis:7.2`）。

### 10.1 黄金用例（integration/，每条 = 构造 + 断言，全部 -race 下跑）

| # | 用例 | 构造 | 断言 |
|---|---|---|---|
| T1 | 双重预订不可能 | 1 任务，32 goroutine 并发 `Reserve(1)` | 恰好 1 个非空返回，31 个空 |
| T2 | enqueue 幂等 | 同 id enqueue ×2 | 第二次 `ErrDup`，ZCARD 仍 1 |
| T3 | 租约到期重投 | vis=100ms，reserve 后不 complete，等 sweep | 二次 reserve 返回同 id，`Tries=2`，`Ver=3`（勘误 #7 算术） |
| T4 | fencing | 承 T3，旧 token（Ver=1）调 complete/heartbeat/fail | 一律 `ErrFenced` |
| T5 | complete 幂等 | complete 成功后同 token 再 complete | `OK_DUP`（nil），`kiki_complete_total{result=dup}`+1 |
| T6 | fail 退避 | fail(err, backoff=1s) | task 进 sched，score≈now+1s；state=SCHEDULED；lease 清空 |
| T7 | 毒丸·fail 路径 | MaxRetries=2，连续三轮 reserve+fail | 第三次 `DeadLettered`；XLEN dlq=1；XRANGE 快照含 payload/err/tries/via=fail |
| T8 | 毒丸·sweep 路径 | MaxRedeliveries=1，反复领了不 ack，等 sweep×2 | 第二轮 sweep 后进 DLQ，`err=lease_exceeded`，`via=sweep` |
| T9 | 优雅下线 | draining 时 1 个在途任务 | 收到 Release，tries 不变、ver 不变（release 不 bump），立即可被重新 reserve（re-reserve 后 Tries=2、Ver=2，区别于 sweep 的 3） |
| T10 | 延迟任务 | delay=500ms enqueue | promote 前 reserve 为空，到点后可领 |
| T11 | NonRetryable | handler 返回 `NonRetryable(err)` | `tries=1` 即进 DLQ，`via=abandon` |
| T12 | Cluster 冒烟 | 4-master cluster（compose，`KIKI_TEST_CLUSTER_ADDRS`；announce IP 必须可路由，见 docker-compose.test.yml 头注） | T1/T3/T7 复跑通过（hash tag 路由验证） |
| T13 | 进程崩溃 | reserve 后 kill -9 测试进程（CI 用 fork 子进程模拟） | 租约到期重投，ver 递增，无孤儿 lease 条目残留 |
| T14 | 分片路由与分布（v0.2） | `ShardedQueue(N=4)` 入队 40+ 任务；`WithRouteKey` 同键多任务；EnqueueBulk | task hash 只存在于 `ShardOf(id)` 预测分片；同 route-key 恒同分片；重复入队 `ErrDup`；bulk 逐条落位 |
| T15 | 跨分片并发恰好一次（v0.2） | 400 任务 × 32 消费者对 `ShardedQueue(4)` 并发 reserve | 恰好 400 次唯一领取；`Task.Shard` 句柄与路由函数一致 |
| T16 | manifest 治理（v0.2） | 初建 N=2；N=4/N=8 漂移构造；SetManifest 扩 2→4；缩 4→2 非空/force | Strict 拒绝启动且带修复指引；Warn 放行；扩容后旧 N 被拒、新分片收新任务；非空缩容被拒、force 放行 |
| T17 | 运维面聚合（v0.2） | 即投 8 + 延迟 4 + 4 分片各 1 毒丸 | Stats 聚合=Σ分片；ListDLQ 合并并带 Shard 标注；ReplayDLQ 按句柄回原分片；SweepOnce 扇出幂等 |

**等价性矩阵（v0.2，integration/sharded_test.go 的 `TestShardedEQ*` 系列）**：把 T1/T2/T3/T4/T5/T6/T7/T8/T9/T10/T11 的核心不变式逐字复刻在 `ShardedQueue(N=4)` 上（双重预订、入队幂等、`Ver=2k−1`、ErrFenced 全家、complete 幂等、双毒丸路径、release 算术、延迟投递、abandon）。设计上未对 T1–T13 做字面参数化——黄金用例体直接读单队列 key 布局，而分片视图的 key 布局按分片展开，两类断言无法共用测试体；等价性以"同一不变式在合并视图上逐字成立"的形式验证。

### 10.2 工程保障

- `go test -race ./...` 为 CI 门禁；黄金用例 `-count=10` 抓 flaky；
- 故障注入：心跳断网（用 `httptest` 式的 net 断开包装 transport）、Redis 重启（compose restart）验证 §4.3/§4.4 结果矩阵；
- 基准：`go test -bench`（单机 Redis 期望 reserve+complete ≥ 8k ops/s），并用 `redis-benchmark` 建立同机基线对照，防止"SDK 慢"误判为"队列慢"。实测数据见 **docs/benchmarks.md**。

#### 10.2.1 基准实测（v0.1.0，2026-08-31，摘录）

| 基准 | 时延 | 吞吐 |
|---|---|---|
| `Enqueue`（单协程） | 199µs/次 | ~5.0k ops/s |
| `ReserveComplete`（单协程） | 393µs/对 | ~2.5k 对/s |
| `ReserveCompleteParallel`（10 并发槽位） | 114µs/对 | **~8.8k 对/s（17.6k ops/s）** |

门槛达标；时延由 Docker Desktop VM 网络 RTT 支配，原生 Linux 按比例改善。方法论与复现方式见 docs/benchmarks.md。

---

## 11. kikictl（运维 CLI，复用 Queue 公共 API）

| 命令 | 作用 |
|---|---|
| `kikictl stats <q>` | ready/sched/lease/dlq 深度 + oldest_ready_age |
| `kikictl inspect <q> <task_id>` | HGETALL 任务全字段（ forensic：tries/ver/last_error/lease_resets） |
| `kikictl enqueue` | `--id --pri --delay --body @file` 手工投递 |
| `kikictl dlq ls <q>` / `dlq inspect` | XRANGE 浏览死信快照 |
| `kikictl dlq replay <q> [--filter k=v] [--force] [--dry-run]` | 重投：读快照重新 enqueue；`--force` 先 DEL 残留 hash（保留期内 replay 需显式授权）；dry-run 只打印计划 |
| `kikictl sweep <q> --limit 200` | 手动触发一轮 sweep（sidecar/救火模式） |

---

## 12. 版本化与滚动升级

- **脚本版本随库走**：`scripts/` 内嵌即编译期冻结；启动时 `Queue.Version()` 汇报（供 kikictl 探测混部版本）；
- **升级纪律**：脚本只加字段/加分支，不改已有字段语义；task hash 增加 `sv`（schema version）字段，未来迁移脚本按 `sv` 分批 HSET——滚动升级期间新旧 worker 共存，新旧脚本对同一状态机的写必须互相兼容（新增字段天然满足；改 score 编码这类破坏性变更加主版本号，要求先清空队列）；
- API 兼容遵循 semver；`ErrFenced` 等哨兵错误的语义承诺永不变。

---

## 13. 实施里程碑

| 里程碑 | 内容 | DoD |
|---|---|---|
| M1（约 1 周） | internal/rdb + 脚本落盘 + Queue 引擎 API | T1–T7、T10 全绿（-race） |
| M2（约 1.5 周） | Worker 运行时：slot loop / heartbeat / Terminator / Shutdown / backoff | T3/T4/T8/T9/T11/T13 全绿；`go test -race -count=10` 无 flaky |
| M3（约 1 周） | Scheduler、中间件、metrics/hooks、kikictl | 指标齐备；dlq replay 干跑通过 |
| M4（约 0.5 周） | Cluster 环境测试、基准报告、godoc/示例 | T12 通过；基准达标（≥8k ops/s 单队列）；v0.1.0 打标 |

**M1–M4 已全部落地**（见 README「状态」节的验证记录）。

---

## 14. 附录：5 分钟上手

```go
// 生产者
rdb := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{"127.0.0.1:6379"}})
q, err := kiki.NewQueue(kiki.QueueOptions{Redis: rdb, Name: "emails"})
body, _ := json.Marshal(EmailJob{To: "a@b.c", Tpl: "welcome"})
err = q.Enqueue(ctx, "email:123:welcome", body,
    kiki.WithPriority(1), kiki.WithMaxRetries(3))

// 消费者
w := q.NewWorker(kiki.WorkerOptions{
    Concurrency: 16,
    Middleware: []kiki.Middleware{
        middlewares.Timeout(30 * time.Second),
        middlewares.Dedup(rdb, 24*time.Hour),
    },
    Hooks: kiki.Hooks{OnDLQ: func(t kiki.Task, via string, cause error) {
        slog.Error("task dead-lettered", "id", t.ID, "via", via, "err", cause)
    }},
})
w.Handle(func(ctx context.Context, t kiki.Task) error {
    var job EmailJob
    if err := json.Unmarshal(t.Payload, &job); err != nil {
        return kiki.NonRetryable(err) // 坏消息不重试，直接 DLQ
    }
    return sendEmail(ctx, job)            // nil → complete；err → 退避重试
})
err = w.Run(ctx)      // 阻塞；ctx 取消即开始优雅下线
w.Shutdown(shutdownCtx)
```

生命周期一句话：`Run` 拉起 slot loop，`ctx.Done`/`Shutdown` 触发 draining → 在途处理 → Release 收尾；任务的状态迁移全程只发生在 Redis 脚本里，Go 侧没有任何一条绕行的写路径。
