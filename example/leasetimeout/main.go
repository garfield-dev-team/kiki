// 技术关键点：超时与租约（Lease / Visibility Timeout）+ 幽灵写防护（fencing）。
//
// 题面要求"维护延迟队列（最小堆或 Redis Sorted Set）管理租期，超时未
// Heartbeat/Complete 的 Task 自动重置状态"。kiki 的取舍与实现：
//   - 延迟/租约索引用 **Redis Sorted Set**（lease ZSET，score = 租约到期时刻），
//     不是内存最小堆：堆随进程死掉、无法多实例共享、无法持久化；
//     ZSET 的 ZRANGEBYSCORE 是确定性扫描——到期的任务一定在结果里（design.md §4.1/4.4）。
//   - sweeper 无主幂等（任何 worker 都能跑，多跑无害，无需选主），自动把
//     超时任务 RESERVED → READY 重投，并 ver+1 毒杀旧 token。
//   - "复活"的旧 worker 回来写 → ErrFenced：幽灵写在物理上被拒绝。
//
// 运行：go run ./example/leasetimeout
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/garfield-dev-team/kiki"
	"github.com/garfield-dev-team/kiki/example/internal/demo"
)

func main() {
	q, c := demo.Setup("leasetimeout")
	defer q.Close()
	ctx := context.Background()
	stop := demo.RunScheduler(q)
	defer stop()

	leaseKey := "{qk:" + q.Name() + "}:lease"

	if err := q.Enqueue(ctx, "slow-job", []byte(`{"work":"long"}`)); err != nil {
		demo.Fatal("enqueue", err)
	}

	// ---- 1. worker 领取后"睡着"：不心跳、不完成 ----
	tasks, err := q.Reserve(ctx, 300*time.Millisecond, 1) // vis=300ms：故意配小，超时立刻可见
	if err != nil || len(tasks) != 1 {
		demo.Fatal("reserve", err)
	}
	t := tasks[0]
	n, _ := c.ZCard(ctx, leaseKey).Result()
	demo.Check(n == 1 && demo.State(q, c, t.ID) == "RESERVED",
		"领取：lease ZSET（score=租约到期时刻）记 1 条，state=RESERVED, ver=%d", t.Ver)

	// ---- 2. 租约到期：sweeper 自动重置状态并毒杀旧 token ----
	demo.Await("租约到期自动重置", 5*time.Second, func() bool {
		return demo.State(q, c, t.ID) == "READY" && demo.Field(q, c, t.ID, "ver") == "2"
	})
	demo.Check(true,
		"超时重置：未心跳/未完成 → sweeper 自动 RESERVED → READY，ver=2（旧 token 被毒杀）")
	n, _ = c.ZCard(ctx, leaseKey).Result()
	demo.Check(n == 0, "租约索引：到期条目已被清出 lease ZSET（无孤儿残留）")

	// ---- 3. "复活"的旧 worker 回来写：幽灵写被物理拒绝 ----
	hbErr := q.Heartbeat(ctx, t, time.Second)
	cpErr := q.Complete(ctx, t, "late result")
	demo.Check(errors.Is(hbErr, kiki.ErrFenced) && errors.Is(cpErr, kiki.ErrFenced),
		"幽灵写防护：旧 token 的 Heartbeat/Complete 一律 ErrFenced（Kleppmann fencing token）")

	// ---- 4. 任务自动重投：新一代 worker 领到，tries=2 ----
	tasks, err = q.Reserve(ctx, 30*time.Second, 1)
	if err != nil || len(tasks) != 1 {
		demo.Fatal("re-reserve", err)
	}
	t2 := tasks[0]
	demo.Check(t2.ID == t.ID && t2.Tries == 2 && t2.Ver == 3,
		"自动重投：同任务被重新领取 tries=2 ver=3（重投路径 ver=2·tries−1）")
	if err := q.Complete(ctx, t2, ""); err != nil {
		demo.Fatal("complete", err)
	}

	fmt.Println("\n技术关键点覆盖：Sorted Set 租约索引 + 无主幂等 sweeper 自动重置 + fencing token 幽灵写防护")
}
