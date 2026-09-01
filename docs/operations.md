# 部署与运维指南

> kiki 是**库，不是服务**：没有 broker 进程要部署，业务进程 import SDK 直连 Redis。
> 本文回答"Redis 怎么配、指标看什么、出事怎么办"。规范依据：design.md §8–§11、go-implementation.md §9/§11。

## 1. 部署形态

```
业务进程 A（Producer + Worker + 内嵌 Scheduler）──┐
业务进程 B（Producer + Worker + 内嵌 Scheduler）──┼──► Redis（broker）
kikictl / sidecar（运维通道）──────────────────┘
```

- **Worker 默认自动内嵌 Scheduler**（首个实例），多实例并发跑 sweep/promote 无害（幂等无主）。若所有进程都关掉 Scheduler 且无人接手，SCHEDULED 与超时租约会滞留——这是配置错误，靠 `kiki_oldest_ready_seconds` 与 `kiki_depth{kind="lease"}` 告警暴露，不要靠造主节点来掩盖。
- 版本探测：`kikictl version` 汇报库版本与 schema 版本；滚动升级纪律见 §6。

## 2. Redis 配置建议

| 项 | 建议 | 理由 |
|---|---|---|
| 持久化 | AOF `appendfsync everysec` | 队列语义下不用 `always`：吞吐代价不成比例，尾部 ~1s 风险由消费侧幂等覆盖 |
| 高可用 | 主从 + Sentinel，`down-after-milliseconds` 调小 | 缩短不可用窗口 |
| Cluster | 单队列 keys 共享 hash tag `{qk:<name>}` ⇒ 同 slot；热队列用 `name#0..N` 子分片 | 单队列吞吐上限 = 单分片上限 |
| `WAIT` | 对丢不起的 complete，客户端可叠加 `WAIT 1 0` | Sentinel 异步复制可丢尾部写入——**不把 Redis 说成 CP 系统是底线** |

## 3. 日常运维：kikictl

```bash
kikictl -addr 127.0.0.1:6379 stats emails        # ready/sched/lease/dlq 深度 + oldest_ready_age
kikictl inspect emails email:123:welcome         # 任务全字段（tries/ver/last_error/lease_resets）
kikictl enqueue emails --id email:999:manual --body @job.json --pri 1 --delay 30s
kikictl dlq ls emails --count 50                 # 死信快照浏览
kikictl dlq replay emails --filter via=fail --dry-run
kikictl dlq replay emails --filter via=fail --force   # 保留期内残留 hash 需显式授权
kikictl sweep emails --limit 200                 # 救火：手动一轮 sweep+promote
```

- `dlq replay` 后端是 `replay.lua`：仅 DLQ 态任务可原子清残留 hash 重放（新 tries 周期）；在途任务一律拒绝。
- `inspect` 是 forensic 工具：`ver` 与 `tries` 对照可判断任务处于第几条投递路径（不变式：第 k 次投递 `ver=2k−1`，release 路径另计）。

## 4. 监控与告警

| 指标 | 含义 |
|---|---|
| `kiki_depth{kind="ready/sched/lease"}` | 三类深度（ZCARD） |
| `kiki_oldest_ready_seconds` | 最老 ready 任务年龄 |
| `kiki_enqueue_total{result=ok/dup/err}` | 生产结果 |
| `kiki_reserve_total{result=ok/empty/err}` | 消费领取结果 |
| `kiki_complete_total{result=ok/dup/fenced}` | complete 结果；`dup` 是响应丢失自愈的证据 |
| `kiki_fail_total{result=retry/dlq/fenced}` | fail 分岔 |
| `kiki_fenced_total{op=hb/complete/fail}` | ★ fencing 拒绝计数 |
| `kiki_dlq_total{via=fail/sweep/abandon}` | 死信来源 |
| `kiki_sweep_requeued_total` | 租约重投量 |
| `kiki_handler_duration_seconds` | handler 直方图（`middlewares.Metrics()` 启用） |

**两条关键告警线（design.md §10）+ 一条纪律：**

1. `kiki_oldest_ready_seconds` 持续增长 → 消费能力不足 / Scheduler 失联 / worker 全灭；
2. `rate(kiki_fenced_total[5m])` 突增 → vis 配小了或 worker 频繁 GC/分区——这是"用户感知到重复执行之前"唯一的前哨信号；
3. `rate(kiki_dlq_total[5m]) > 0` 持续 → 死信必须有人处理，DLQ 是待办事项不是垃圾桶。

Hooks（`OnDLQ/OnFenced/OnPanic/OnHeartbeatLost`）负责"哪个"：接告警路由与采样日志。

## 5. 容量规划速查

- **内存**：task hash ≈ payload + ~300B 元数据；ZSET entry ≈ 100B。100 万在途（1KB payload）≈ 1.5GB。**payload > 100KB 必须改存对象存储引用**——纪律不是建议。
- **RTT 预算**：enqueue 1 + reserve 1（批量摊薄）+ heartbeat ⌈时长/(vis/3)⌉ + complete/fail 1。
- **吞吐**：单队列 = 单分片上限，实测见 [docs/benchmarks.md](benchmarks.md)；扩容走子分片。
- **保留期**：终态 task hash 靠 EXPIRE（默认 24h）；DLQ Stream `XTRIM MAXLEN ~` 封顶（默认 10000）；历史审计依赖 Stream 而非 hash 永生。

## 6. 升级与兼容

- 脚本随库编译期冻结（`go:embed`），`Queue.Version()` / `kikictl version` 汇报混部版本；
- task hash 带 `sv`（schema version）字段：未来迁移按 `sv` 分批；
- 升级纪律：脚本只加字段/加分支，不改已有字段语义；破坏性变更（如 score 编码）必须升主版本并先清空队列；
- `ErrFenced` 等哨兵语义承诺永不变（semver）。

## 7. 故障 Runbook

| 症状 | 先看 | 处置 |
|---|---|---|
| ready 深度涨、worker 空转 | `kikictl stats`、oldest_ready_age | 加 Worker 实例/并发；确认内嵌 Scheduler 没被全部关闭 |
| `kiki_fenced_total` 突增 | `kikictl inspect <q> <id>` 的 tries/ver、handler 时长 vs vis | 调大 vis（仅限下调上限内）或修 handler 长尾；长任务靠心跳不是调大 vis |
| 任务卡 SCHEDULED 不动 | 有没有进程在跑 promote | 启动任一 Worker 或 `kikictl sweep <q>` |
| lease 深度高、无人认领 | worker 是否崩溃后未重启 | sweep 会自动重投；确认 `MaxRedeliveries` 未被反复触发进 DLQ |
| DLQ 堆积 | `kikictl dlq ls` | 修复根因 → `dlq replay --dry-run` 预演 → replay（必要时 `--force`） |
| Redis 主从切换后任务重投 | AOF/复制状态 | at-least-once 语义正常工作：靠消费侧幂等收敛；对丢不起的 complete 叠加 `WAIT` |
