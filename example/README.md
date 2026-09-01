# example — kiki 最小可跑示例

一条命令走完任务队列的完整生命周期：投递 → 延迟任务 promote → 瞬时失败退避重试 → 毒丸直达死信 → 优雅下线。

## 运行

```bash
# 1. 起一个本地 Redis（唯一前置依赖）
docker run --rm -p 6379:6379 redis:7.2

# 2. 跑示例
go run ./example
```

Redis 不在默认地址时：`REDIS_ADDR=host:port go run ./example`。

## 预期输出（节选）

```text
已投递 4 个任务（welcome / digest[延迟300ms] / bad[毒丸] / flaky[失败2次]）
>>> DLQ: task=email:u3:bad via=abandon cause=unmarshal payload: ...
>>> 处理完成: task=email:u1:welcome tries=1 ver=1
>>> 处理完成: task=email:u2:digest tries=1 ver=1
>>> 处理完成: task=email:u4:flaky tries=3 ver=5
终态: ready=0 sched=0 lease=0 dlq=1
死信快照: id=email:u3:bad via=abandon tries=1 err="unmarshal payload: ..."
```

读点：

- **digest** 走了 `enqueue(delay>0) → SCHEDULED → promote → reserve` 的完整链路；
- **flaky** 的 `tries=3, ver=5` 验证 fencing 不变式（第 k 次投递 `ver=2k−1`，两次 fail 各 +1、三次 reserve 各 +1）；
- **bad** 是毒丸：`NonRetryable` 让首投即进 DLQ（`via=abandon`），不浪费一次重试；
- 终态 `dlq=1` 来自内嵌 Scheduler 的无主 sweep/promote 循环与 fail 分岔。

## 与生产接入的距离

本示例为可读性做了简化，生产接入请对照 [docs/sdk.md](../docs/sdk.md)：

- 加 `middlewares.Dedup`（消费侧幂等，at-least-once 语义的另一半）；
- 配 `Hooks`（OnDLQ/OnFenced/OnPanic/OnHeartbeatLost）接告警；
- 退避用默认 `ExponentialBackoff(1s, 60s, 0.5)`（本示例缩小了参数只为演示可见）；
- 指标接 Prometheus（`QueueOptions.MetricsRegisterer`）。
