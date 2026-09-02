// 考核点：状态机设计（State Machine）。
//
// kiki 的状态机（design.md §3，与题面对应）：
//
//	Unassigned = READY / SCHEDULED        终态 = COMPLETED / DLQ
//	READY  --reserve-->  RESERVED  --complete-->  COMPLETED
//	RESERVED --fail(tries≤max)--> SCHEDULED --promote--> READY   （fail 是事件不是状态）
//	RESERVED --fail(tries>max)--> DLQ（见 example/poisonpill）
//
// 全部转移收敛为 Lua 原子脚本（单一写入者），无 FAILED 停留态。
// 本例把每一步之后 Redis 里的真实 state 打出来——一张可执行的状态转移表。
//
// 运行：go run ./example/statemachine
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
	q, c := demo.Setup("statemachine")
	defer q.Close()
	ctx := context.Background()
	stop := demo.RunScheduler(q) // promote：SCHEDULED 到期提升回 READY
	defer stop()

	// ---- 1. enqueue(delay=0) → READY（Unassigned）----
	if err := q.Enqueue(ctx, "email:1:welcome", []byte(`{"tpl":"welcome"}`)); err != nil {
		demo.Fatal("enqueue", err)
	}
	printStep("enqueue(delay=0)", "READY", demo.State(q, c, "email:1:welcome"), "ZADD ready")

	// ---- 2. enqueue(delay>0) → SCHEDULED，到期 promote → READY ----
	if err := q.EnqueueIn(ctx, "email:2:digest", []byte(`{"tpl":"digest"}`), 200*time.Millisecond); err != nil {
		demo.Fatal("enqueueIn", err)
	}
	printStep("enqueue(delay>0)", "SCHEDULED", demo.State(q, c, "email:2:digest"), "ZADD sched(visible_at)")
	demo.Await("promote 到期提升", 5*time.Second, func() bool {
		return demo.State(q, c, "email:2:digest") == "READY"
	})
	printStep("promote(到期)", "READY", demo.State(q, c, "email:2:digest"), "sched → ready")

	// ---- 3. reserve → RESERVED（tries+1，ver+1）----
	tasks, err := q.Reserve(ctx, 30*time.Second, 1)
	if err != nil || len(tasks) != 1 {
		demo.Fatal("reserve", err)
	}
	t := tasks[0]
	printStep("reserve", "RESERVED", demo.State(q, c, t.ID),
		fmt.Sprintf("tries=%d ver=%d", t.Tries, t.Ver))

	// ---- 4. fail(tries≤max) → SCHEDULED（fail 是事件不是状态）----
	res, err := q.Fail(ctx, t, errors.New("transient"), 100*time.Millisecond)
	if err != nil || res != kiki.Retried {
		demo.Fatal("fail", err)
	}
	printStep("fail(可重试)", "SCHEDULED", demo.State(q, c, t.ID),
		fmt.Sprintf("ver=%s（旧 token 被毒杀）", demo.Field(q, c, t.ID, "ver")))
	demo.Await("退避后 promote", 5*time.Second, func() bool {
		return demo.State(q, c, t.ID) == "READY"
	})

	// ---- 5. 二次 reserve → RESERVED（新一轮投递）----
	tasks, err = q.Reserve(ctx, 30*time.Second, 1)
	if err != nil || len(tasks) != 1 {
		demo.Fatal("re-reserve", err)
	}
	t = tasks[0]
	demo.Check(t.Tries == 2 && t.Ver == 3,
		"转移 reserve(重投)             READY → RESERVED  tries=2 ver=3（不变式 ver=2·tries−1）")

	// ---- 6. complete → COMPLETED（终态）----
	if err := q.Complete(ctx, t, ""); err != nil {
		demo.Fatal("complete", err)
	}
	printStep("complete", "COMPLETED", demo.State(q, c, t.ID), "ZREM lease，保留期后过期")

	// ---- 7. 终态语义：重复 complete 幂等；终态不可再领取 ----
	demo.Check(q.Complete(ctx, t, "") == nil, "转移 complete(重复)           COMPLETED → COMPLETED（OK_DUP 幂等）")
	again, _ := q.Reserve(ctx, time.Second, 1)
	demo.Check(len(again) == 0, "终态封口：COMPLETED 后 Reserve 返回空（无 FAILED 停留态，无僵尸）")

	fmt.Println("\n考核点覆盖：状态机 = 转移表由 Lua 脚本原子实现；fail 是事件（重试/DLQ 分岔），Completed/Failed 对应 #6/#7+#8")
}

func printStep(op, want, got, note string) {
	demo.Check(want == got, "转移 %-28s → %-9s %s", op, got, note)
}
