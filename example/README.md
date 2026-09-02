# example — kiki 示例集

本目录服务两个目的：**5 分钟上手**（[`main.go`](main.go)）与**面试考核点逐项对照**（下列子目录）。
每个示例都是独立可运行的 `main`，对着真实 Redis 打印 ✓/✗ 判定；全部写路径走 SDK（Lua 脚本），
示例只读直查 Redis 里的 state/ver 作证据。

## 运行

```bash
docker run --rm -p 6379:6379 redis:7.2   # 唯一前置依赖
go run ./example                          # 5 分钟上手：完整流水线
go run ./example/concurrency              # 考核点：并发控制
go run ./example/statemachine             # 考核点：状态机设计
go run ./example/leasetimeout             # 技术关键点：超时与租约（自动重置）
go run ./example/poisonpill               # 技术关键点：死信队列与毒丸防护
go run ./example/sandbox                  # 场景应用：Agent Sandbox（资源池 + 三条事件队列）
```

Redis 不在默认地址时：`REDIS_ADDR=host:port go run ./example/<dir>`。
每个示例创建带时间戳的独立队列，重复运行互不干扰；演示数据保留在 Redis，
可用 `kikictl stats <队列名>` / `kikictl inspect <队列名> <任务id>` 事后复查。

## 面试考核点对照

考核要求：核心操作 **reserve / complete / fail**，任务在
**Unassigned → Reserved → Completed / Failed / DLQ** 间流动；
技术关键点：**超时与租期**（Sorted Set 延迟队列，超时自动重置）、**死信队列**（retry 阈值防毒丸）。

| 考核点 / 关键点 | 示例 | 亲自观察 | 对应黄金用例* |
|---|---|---|---|
| reserve / complete / fail 核心操作 + 状态流动 | `statemachine` | 逐步打印 Redis 里的真实 state：READY → RESERVED → SCHEDULED →（promote）→ RESERVED → COMPLETED | T3/T6/T10 |
| 并发控制 | `concurrency` | 32 goroutine 并发 Reserve 1 个任务恰好 1 个成功；同 id Enqueue → ErrDup；同 token 重复 Complete → OK_DUP | T1/T2/T5 |
| 超时与租期（延迟队列 + 自动重置） | `leasetimeout` | vis=300ms 不心跳 → sweeper 自动 RESERVED→READY、ver+1；"复活"的旧 worker 写入 → ErrFenced | T3/T4/T13 |
| 死信队列与毒丸 | `poisonpill` | 三条 DLQ 路径全跑通：tries>MaxRetries（fail）、lease_resets 超限（sweep，防"杀死 worker"型毒丸）、NonRetryable（abandon） | T7/T8/T11 |

\* 黄金用例是这些演示的测试化形态（`go test -tags=integration ./integration/`，真实 Redis，`-race -count=10` 门禁）——演示给人看，用例给 CI 看，断言同源。

## 与生产接入的距离

- 加 `middlewares.Dedup`：at-least-once 语义下，重复投递由消费侧幂等收敛（[docs/sdk.md §4](../docs/sdk.md)）；
- 配 `Hooks`（OnDLQ/OnFenced/OnPanic/OnHeartbeatLost）接告警；`kiki_fenced_total` 突增是重复执行的前哨；
- 退避用默认 `ExponentialBackoff(1s, 60s, 0.5)`（示例缩小参数只为演示可见；抖动不可省）；
- 指标接 Prometheus（`QueueOptions.MetricsRegisterer`），告警线见 [docs/operations.md §4](../docs/operations.md)。

## 场景应用：Agent Sandbox（`sandbox/`）

按 [agent-sandbox-task-queue.md](../agent-sandbox-task-queue.md) 的建模把 kiki 用到 Manus 类产品：
**沙箱不是任务，是被租用的资源**——一个资源池 + 三条事件队列，sandbox API 用 mock（生产替换为 Docker/K8s 调用）：

| 队列原语 | 沙箱语义 | 演示章节 |
|---|---|---|
| `reserve` | 给会话分配容器（bind.lua 原子配对，两个用户绝不可能拿到同一容器） | 第 2 章：VIP（tier=0）先于 free；分配互斥 |
| `enqueue`+delay | 预热错峰（预测早高峰提前排供给任务） | 第 1 章：sbx-3 延迟 300ms |
| `fail` 退避 | 供给失败（镜像拉取）→ 重试；超限 → DLQ | 第 1 章：sbx-2 重试成功；sbx-bad 进 DLQ |
| `heartbeat` | 会话续约（生产：挂在 agent API 调用上） | 第 3 章：网关 ♥ 续租 |
| `complete` | 会话正常结束 → reset 队列 sanitize 后新一代回池（回收 ≠ 直接复用） | 第 3 章：sbx-1-g2 |
| `sweep`+fencing | 僵尸容器回收：心跳停 → 强杀 → 回池；旧会话复活 → ERR_FENCED → 410 | 第 4 章 |
| `lease_resets` 上限 | 启动即崩的容器停止「分配→崩溃→回收→再分配」循环 → DLQ | 第 5 章 |

唯一新写的脚本是 [`sandbox/bind.lua`](sandbox/bind.lua)（Dispatcher 原子配对）——
"在原状态机上做加法，不改地基"。诚实条款：bind.lua 跨 hash tag 仅单机/主从 Redis 合法，
生产 Cluster 需同 slot 对齐；`max_hold` 绝对持有上限、放置（bin-packing）、单用户公平性见文档 §四 差距清单。

## 目录结构

```
example/
├── main.go            # 5 分钟上手：完整流水线
├── internal/demo/     # 共用脚手架（连接/断言/等待，示例专用）
├── concurrency/       # 考核点：并发控制
├── statemachine/      # 考核点：状态机设计
├── leasetimeout/      # 技术关键点：超时与租约
├── poisonpill/        # 技术关键点：死信队列与毒丸
└── sandbox/           # 场景应用：Agent Sandbox（bind.lua + 资源池 + 三条事件队列）
```
