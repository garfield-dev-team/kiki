# 基准测试报告（kiki v0.1.0）

> 复现方式与读数纪律。数据快照：2026-08-31，module `github.com/garfield-dev-team/kiki`。

## 1. 环境

| 项 | 值 |
|---|---|
| 硬件 | Apple Silicon（M 系，10 P-core，arm64） |
| 宿主 OS | macOS 25.3.0 |
| Redis | redis:7.2 容器（Docker Desktop VM 网络） |
| payload | 1 字节（隔离 RTT 成本；1KB 影响见 design.md §9 内存模型） |
| 持久化 | 无 AOF 压力（基准测的是执行路径，不是 fsync） |

## 2. 数据

```
go test -tags=integration -bench=. -benchtime=3s ./integration/
```

| 基准 | 时延 | 吞吐 | 说明 |
|---|---|---|---|
| `BenchmarkEnqueue` | 199µs/次 | ~5.0k ops/s | 1 次串行 RTT（EVALSHA enqueue.lua） |
| `BenchmarkReserveComplete` | 393µs/对 | ~2.5k 对/s | 2 次串行 RTT（reserve + complete） |
| `BenchmarkReserveCompleteParallel` | 114µs/对 | **~8.8k 对/s（17.6k ops/s）** | 10 并发槽位摊薄 RTT |

## 3. 读数要点（对照 design.md §9 预算模型）

1. **时延由 RTT 支配，不是 Redis 执行**。单次脚本 RTT 在 Docker Desktop VM 网络上 ≈ 200µs；原生 Linux 同版本 Redis 通常 < 100µs，各数字按比例改善。**结论以"≥8k ops/s 门槛达标"为准，不以绝对值为准。**
2. **单协程 reserve+complete ≈ 2×enqueue 时延**，与预算模型"reserve 1 + complete 1"精确一致——SDK 没有引入模型外的 RTT。
3. **并发吞吐随槽位数近线性增长**，直至 Redis 单分片脚本执行上限。吞吐扩容路径是队列子分片（`orders#0..N`，go-implementation.md §3.4），不是把 Concurrency 调到无穷。
4. **心跳摊销**：10 分钟任务、vis=30s ≈ 3 次 heartbeat RTT；基准未含心跳（短任务形态），长任务按 §9 预算另计。

## 4. CI 对照建议（§10.2 的"防误判"纪律）

固定同机 `redis-benchmark` 脚本基线，SDK 吞吐 / 裸脚本吞吐 > 0.7 视为"SDK 未退化"；回退 > 20% 必须书面说明（AGENTS.md §6）。

```bash
# SDK 侧
go test -tags=integration -bench=BenchmarkReserveCompleteParallel -benchtime=10s ./integration/
# 基线（同机、同 payload）
redis-benchmark -n 100000 -P 16 -t evalsha
```

## 5. 已知边界

- 单队列吞吐上限 = 单 Redis 分片上限（hash tag 决定，design.md §4.3）；要横向扩展先分子分片。
- 本报告不含 Cluster 吞吐：T12 只做正确性冒烟，跨分片扩展的吞吐曲线待 v0.2 ShardedQueue 落地后补测。
