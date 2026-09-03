# 基准测试报告（kiki v0.2.0）

> 复现方式与读数纪律。数据快照：2026-08-31（v0.1.0 单队列）/ 2026-09-03（v0.2.0 分片），module `github.com/garfield-dev-team/kiki`。

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

## 3.1 分片扩展性实测（v0.2.0，2026-09-03，`BenchmarkShardedReserveCompleteParallel`）

环境同 §1，另起 4-master compose 集群（`KIKI_TEST_CLUSTER_ADDRS`）。`ShardedQueue(N=4)` 对照单队列，梯度并发（`-cpu=10,40,80`）：

| 并发客户端 | 单队列（1 slot/1 master） | ShardedQueue N=4 | 分片相对增益 |
|---|---|---|---|
| 10 | 112µs/对 ≈ 8.9k 对/s | 144µs/对 ≈ 7.0k 对/s | **−22%**（集群客户端路由开销可见） |
| 40 | 78µs/对 ≈ 12.9k 对/s | 75µs/对 ≈ 13.2k 对/s | ≈持平（交叉点） |
| 80 | 72µs/对 ≈ 13.9k 对/s（**饱和**） | 60µs/对 ≈ **16.8k 对/s（仍在爬升）** | **+21%** |

读数要点：

1. **曲线形状符合设计预期，不是绝对倍数**：单队列在 ~14k 对/s 触及单分片上限后饱和（40→80 并发仅 +8%）；分片在 80 并发仍 +27% 增长——"加消费者无效"的饱和点被真实推后了。这是 §3.1 要证明的核心命题。
2. **低并发区 −22% 是集群客户端的固有开销**（slot 映射、按节点连接池、VM 桥接 RTT 方差），不是 SDK 路由的开销（路由 = 一次 fnv 取模，纳秒级）。给"低流量队列不要上分片"提供了数据依据：容量判定树（operations.md §5）先加消费者、后扩 N 的顺序是对的。
3. **§10.3 的"≥3×"验收目标在本环境不可测**：Docker Desktop 的 4 个 master 共享同一 VM CPU 池，绝对吞吐被宿主封顶。曲线形状（单队列饱和 vs 分片爬升）是本环境能给出的最强分布正确性证据；3× 目标留给多宿主机集群验证。
4. 分片基准只在积压态成立（预灌 b.N 任务）：积压下乱序轮询首探测即命中，每对仍是 2 次 RTT——与单队列 RTT 预算一致，**ShardedQueue 没有引入模型外的 RTT**。

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
- 本报告不含 Cluster 吞吐：T12 只做正确性冒烟，跨分片扩展的吞吐曲线待 v0.2 ShardedQueue 落地后补测（方案与验收基准见 [sharded-queue.md](sharded-queue.md) §10）。
