<p align="center">
  <img src=".banner/kiki-banner.webp" alt="kiki — 魔女的宅急便 · 生产级任务队列" />
</p>

# kiki 🧹

> 魔女的宅急便 · 生产级任务队列。
> 基于 Redis 的生产级任务队列——队列中间件 + Go Client SDK。
> at-least-once 投递 · Lua 原子状态机 · fencing token 防幽灵写 · 双路径毒丸防线汇入 DLQ · 无主 sweeper（回收器无单点）。

## 名字由来

名字取自宫崎骏《魔女宅急便》（魔女の宅急便 / Kiki's Delivery Service）。日文片名里的"宅急便"本就是快递服务的意思——这部动画讲的是一个魔女少女靠"送货"立足成长的故事，主题恰好就是**任务投递这份工作本身**。用它来命名任务队列，片中的角色映射几乎严丝合缝：

| 动画里 | 队列里 |
|---|---|
| Kiki 骑扫帚出门送件 | worker 领取（reserve）任务并处理 |
| 要送的件 | Task |
| 扫帚——一次只能骑一把 | Lease / 租约：领取即持有，且同一任务同时只有一个持有者 |
| 沿途向面包店老板娘报平安 | Heartbeat 续约 |
| Kiki 半途睡着，件由别的魔女接力送出；她醒来后手里那把"旧扫帚"已作废 | 租约到期 sweep 重投 + fencing token（`ver`）毒杀旧持有者 |
| 怎么都送不到的件 | Dead Letter Queue |

顺带的工程考虑：四个字母、全小写、无歧义发音——CLI（`kikictl`）、Prometheus 指标前缀（`kiki_`）、去重键（`kiki:dedup:`）都干净。

命名只关乎品牌，不触及规范：状态机、key 布局与语义定义见 `design.md`（内部 hash tag 仍为 `{qk:<queue>}`，与项目名解耦）。

## 文档地图

| 文件 | 内容 | 读者 |
|---|---|---|
| `design.md` | 技术方案规范：状态机转移表、Lua 原子脚本、竞态消灭矩阵、韧性故障矩阵 | 所有人 |
| `go-implementation.md` | Go 实施方案：工程结构、Worker 并发模型、错误分类学、黄金用例 T1–T13、里程碑 M1–M4、勘误记录（§3.5） | 开发者 |
| `docs/sdk.md` | SDK 接入文档：生产/消费 API、错误分类学、幂等协议、优雅下线、FAQ | 业务接入方 |
| `docs/operations.md` | 部署运维指南：Redis 配置、kikictl、监控告警、容量规划、故障 Runbook | 运维 / SRE |
| `docs/benchmarks.md` | 基准测试报告：数据、方法论、CI 对照纪律 | 所有人 |
| `docs/sharded-queue.md` | ShardedQueue 技术方案（v0.2，方案定稿未实施）：子分片路由、多生产者/多消费者模型、N 治理与迁移 runbook | 开发者 / 架构 |
| `AGENTS.md` | agent / 贡献者的开发行为准则（硬规则与高危陷阱清单） | agent 与人类贡献者 |
| `scripts/`（已交付） | 10 个 Lua 规范脚本——一切状态转移的唯一写路径（含 abandon / replay 运维路径） | 开发者 |
| `example/` | 最小可跑示例：一条命令走完整条流水线（`go run ./example`） | 新人 |

**真相裁决顺序**：`scripts/` + integration 测试 > `design.md` > `go-implementation.md` > `AGENTS.md`。文档与代码冲突时先修文档。

## 状态

**v0.1.0 已实施。** 设计（design.md / go-implementation.md）→ 脚本（scripts/，10 个）→ 验证（integration 黄金用例 T1–T13）三处同步完成。验证记录（2026-08-31，本机 Docker Desktop + redis:7.2）：

- `go build ./...`、`go vet ./...`、`gofmt` 干净；
- `go test -race ./...`（单测，miniredis 仅用于纯 Go 逻辑）通过，`-count=5` 无 flaky；
- `go test -race -tags=integration -count=10 ./integration/`（黄金用例 T1–T13，testcontainers 真实 Redis）通过——T4 fencing、T8 毒丸 sweep 路径、T12 4-master cluster 冒烟、T13 kill -9 崩溃重投在内；
- 基准（同机 Docker Desktop）：单协程 reserve+complete 对 393μs；10 并发槽位 ≈ **8.8k 对/秒（17.6k ops/s）**，超过 §10.2 的 8k ops/s 门槛——Docker Desktop 的 VM 网络抬高 RTT，原生 Linux 上余量更大；
- kikictl（stats/inspect/enqueue/dlq ls|replay/sweep）可用；勘误 10 条见 go-implementation.md §3.5。

## 目标 API（已交付）

```go
q, _ := kiki.NewQueue(kiki.QueueOptions{Redis: rdb, Name: "emails"})
_ = q.Enqueue(ctx, "email:123:welcome", body, kiki.WithPriority(1))

w := q.NewWorker(kiki.WorkerOptions{Concurrency: 16})
w.Handle(func(ctx context.Context, t kiki.Task) error {
    return deliver(ctx, t) // nil → complete；err → 指数退避重试；NonRetryable(err) → 直达 DLQ
})
_ = w.Run(ctx)
```

## 语义承诺（不可协商，详见 design.md §0）

1. **at-least-once，不声称 exactly-once**——重复投递由消费侧 `kiki:dedup:` 幂等键收敛；
2. **`fail` 是事件不是状态**——没有 FAILED 停留态，失败要么退避重排，要么进 DLQ；
3. **一切到期判断用 Redis 服务器时间**，客户端时钟只写日志。

## 快速体验

```bash
docker run --rm -p 6379:6379 redis:7.2
go run ./example
```

面试考核点（并发控制 / 状态机 / 超时租期 / 毒丸 DLQ）的逐项对照示例见 [example/README.md](example/README.md)。
