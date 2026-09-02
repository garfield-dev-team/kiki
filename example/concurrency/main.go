// 考核点：并发控制（Concurrency Control）。
//
// 本例演示三道并发防线，全部来自"原子脚本 + 全量守卫"（design.md §6 竞态消灭矩阵）：
//  1. 双重预订物理不可能 —— reserve.lua 单脚本内 ZPOPMIN 原子弹出，
//     弹出的瞬间任务已离开 ready ZSET，32 个 worker 抢 1 个任务只有 1 个成功；
//  2. 生产者重试幂等 —— 业务 id 作任务 id，enqueue.lua 的 EXISTS 守卫拒绝重复；
//  3. 终结幂等 —— owner+token 匹配下重复 complete 返回 OK_DUP，
//     "complete 成功但响应丢失，客户端重试"被安全吸收。
//
// 运行：go run ./example/concurrency   （前置：本地 Redis，见 example/README.md）
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/garfield-dev-team/kiki"
	"github.com/garfield-dev-team/kiki/example/internal/demo"
)

const workers = 32

func main() {
	q, c := demo.Setup("concurrency")
	defer q.Close()
	ctx := context.Background()

	if err := q.Enqueue(ctx, "job-1", []byte(`{"work":"x"}`)); err != nil {
		demo.Fatal("enqueue", err)
	}

	// ---- 1. 并发 reserve：双重预订物理不可能 ----
	type outcome struct {
		tasks []kiki.Task
		err   error
	}
	outcomes := make(chan outcome, workers)
	for i := 0; i < workers; i++ {
		go func() {
			ts, err := q.Reserve(ctx, 30*time.Second, 1)
			outcomes <- outcome{ts, err}
		}()
	}
	won, winner := 0, kiki.Task{}
	for i := 0; i < workers; i++ {
		o := <-outcomes
		if o.err != nil {
			demo.Fatal("reserve", o.err)
		}
		if len(o.tasks) == 1 {
			won++
			winner = o.tasks[0]
		}
	}
	demo.Check(won == 1, "并发控制：%d 个 goroutine 并发 Reserve 1 个任务，恰好 %d 个领到（ZPOPMIN 原子弹出）", workers, won)
	demo.Check(winner.Ver == 1 && winner.Tries == 1,
		"首次领取颁发 fencing token：ver=%d tries=%d（fencing 纪律：ver 只在 reserve 颁发）",
		winner.Ver, winner.Tries)

	// ---- 2. 生产者幂等：同 id 重复投递 ----
	err := q.Enqueue(ctx, "job-1", []byte(`{"work":"dup"}`))
	demo.Check(errors.Is(err, kiki.ErrDup),
		"生产者幂等：同 id 重复 Enqueue → ErrDup（EXISTS 守卫，生产者重试安全）")

	// ---- 3. 终结幂等：complete 响应丢失后的重试 ----
	if err := q.Complete(ctx, winner, ""); err != nil {
		demo.Fatal("complete", err)
	}
	err = q.Complete(ctx, winner, "")
	demo.Check(err == nil, "终结幂等：同 owner+token 重复 Complete → OK_DUP（nil），不产生二次副作用")

	demo.Check(demo.State(q, c, "job-1") == "COMPLETED", "终态：state=COMPLETED，保留期后过期")

	fmt.Println("\n考核点覆盖：并发控制 = 原子弹出（reserve）+ EXISTS 幂等（enqueue）+ owner+token 幂等终结（complete）")
}
