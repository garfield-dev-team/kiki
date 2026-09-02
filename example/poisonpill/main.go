// 技术关键点：死信队列（DLQ）与毒丸消息（Poison Pill）防护。
//
// "毒丸"有两种杀法，kiki 用两道独立上限把它们都汇入 DLQ（design.md §5.5/5.6）：
//
//	A. fail 路径 —— 任务反复失败：投递计数 tries 超过 MaxRetries 阈值 → DLQ；
//	B. sweep 路径 —— 任务反复"杀死 worker"（OOM、反序列化炸弹），
//	   永远没人调 fail：重投计数 lease_resets 超过 MaxRedeliveries → DLQ；
//	C. abandon 路径 —— NonRetryable 错误（参数非法、4xx 类）重试纯属浪费：
//	   首投即 DLQ，不烧完重试预算。
//
// DLQ 是 Stream（XRANGE 可回放、XTRIM 封顶），快照含 payload/err/tries/via。
//
// 运行：go run ./example/poisonpill
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
	ctx := context.Background()

	// ---- A. 毒丸·反复失败：tries 超过 MaxRetries → DLQ（fail 路径）----
	qa, ca := demo.Setup("poison-fail")
	defer qa.Close()
	if err := qa.Enqueue(ctx, "bad-job", []byte(`{"crash":true}`)); err != nil {
		demo.Fatal("enqueue", err)
	}
	// MaxRetries=2 ⇒ 共投递 3 次（首投 + 2 次重试），第 3 次 fail 进 DLQ。
	for round := 1; ; round++ {
		tasks, err := qa.Reserve(ctx, 30*time.Second, 1)
		if err != nil || len(tasks) != 1 {
			demo.Fatal(fmt.Sprintf("round %d reserve", round), err)
		}
		res, err := qa.Fail(ctx, tasks[0], fmt.Errorf("boom(round %d)", round), time.Millisecond)
		if round < 3 {
			if err != nil || res != kiki.Retried {
				demo.Fatal(fmt.Sprintf("round %d fail", round), err)
			}
			demo.Await(fmt.Sprintf("第 %d 次重投（退避+promote）", round), 3*time.Second, func() bool {
				return demo.State(qa, ca, "bad-job") == "READY"
			})
			continue
		}
		demo.Check(err == nil && res == kiki.DeadLettered,
			"毒丸 A（fail 路径）：tries=3 > MaxRetries=2 → DeadLettered（前两轮 RETRY 已按退避重排）")
		break
	}
	entries, err := qa.ListDLQ(ctx, 10)
	if err != nil || len(entries) != 1 {
		demo.Fatal("list dlq", err)
	}
	e := entries[0]
	demo.Check(e.Via == "fail" && e.Tries == 3,
		"DLQ 快照：via=%s tries=%d err=%q payload=%q（Stream 可 XRANGE 回放）",
		e.Via, e.Tries, e.Err, e.Payload)

	// ---- B. 毒丸·杀死 worker：没人调 fail → lease_resets 超限 → DLQ（sweep 路径）----
	qb, cb := demo.Setup("poison-kill")
	defer qb.Close()
	stopB := demo.RunScheduler(qb)
	defer stopB()
	if err := qb.Enqueue(ctx, "killer-job", []byte(`{"oom":true}`)); err != nil {
		demo.Fatal("enqueue", err)
	}
	// 模拟"领了就崩"：反复领取、从不 ack/fail——租约到期只能靠 sweep 重投。
	dlqB := func() int64 {
		n, _ := cb.XLen(ctx, "{qk:"+qb.Name()+"}:dlq").Result()
		return n
	}
	go func() {
		for dlqB() == 0 {
			_, _ = qb.Reserve(ctx, 100*time.Millisecond, 1) // 领取后"崩溃"（不 ack）
			time.Sleep(10 * time.Millisecond)
		}
	}()
	demo.Await("sweep 路径死信", 15*time.Second, func() bool { return dlqB() == 1 })
	entriesB, _ := qb.ListDLQ(ctx, 10)
	demo.Check(len(entriesB) == 1 && entriesB[0].Via == "sweep" && entriesB[0].Err == "lease_exceeded",
		"毒丸 B（sweep 路径）：反复杀死 worker → lease_resets 超过 MaxRedeliveries → DLQ（err=lease_exceeded）")

	// ---- C. NonRetryable：首投即 DLQ（abandon 路径）----
	qc, _ := demo.Setup("poison-nonretryable")
	defer qc.Close()
	dlqSeen := make(chan string, 1)
	w := qc.NewWorker(kiki.WorkerOptions{
		Concurrency:       1,
		PollInterval:      20 * time.Millisecond,
		SchedulerInterval: 0,
		Hooks: kiki.Hooks{OnDLQ: func(t kiki.Task, via string, cause error) {
			dlqSeen <- via
		}},
	})
	w.Handle(func(ctx context.Context, t kiki.Task) error {
		return kiki.NonRetryable(errors.New("unmarshal: invalid character 'h'")) // 坏消息，重试无意义
	})
	if err := qc.Enqueue(ctx, "broken-job", []byte(`html not json`)); err != nil {
		demo.Fatal("enqueue", err)
	}
	go func() { _ = w.Run(ctx) }()
	select {
	case via := <-dlqSeen:
		demo.Check(via == "abandon", "毒丸 C（abandon 路径）：NonRetryable 首投即 DLQ（tries=1，不烧重试预算）")
	case <-time.After(5 * time.Second):
		demo.Fatal("abandon", errors.New("任务未死信"))
	}
	_ = w.Shutdown(ctx)

	fmt.Println("\n技术关键点覆盖：retry_count(tries) 阈值 → DLQ（A）；worker 杀手型毒丸由 sweep 路径兜底（B）；NonRetryable 直达（C）——DLQ 是待办事项不是垃圾桶")
}
