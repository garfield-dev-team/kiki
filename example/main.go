// example 是 kiki 的最小可跑示例：一条命令走完整条任务队列流水线。
//
// 前置：本地起一个 Redis（无需其它基础设施）：
//
//	docker run --rm -p 6379:6379 redis:7.2
//
// 运行：
//
//	go run ./example
//
// 它会依次演示：
//  1. 普通任务投递与完成（enqueue → reserve → complete）
//  2. 延迟任务（enqueue delay>0 → SCHEDULED → promote → 领取）
//  3. 瞬时失败与指数退避重试（fail → RETRY → 第 3 次成功）
//  4. 毒丸任务直达死信（NonRetryable → abandon.lua → DLQ，via=abandon）
//  5. 无主 Scheduler（内嵌 sweep+promote）、优雅下线（Shutdown）
//
// 语义提醒：本示例是 at-least-once 队列——重复投递由消费侧幂等收敛
// （真实业务请加 middlewares.Dedup，见 docs/sdk.md §4）。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/garfield-dev-team/kiki"
	"github.com/garfield-dev-team/kiki/middlewares"
)

// Job 是示例业务载荷（payload 对队列不透明）。
type Job struct {
	To        string `json:"to"`
	Tpl       string `json:"tpl"`
	FailTimes int    `json:"fail_times,omitempty"` // >0 时前 N 次投递故意失败
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	rdb := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{addr}})
	defer rdb.Close()

	q, err := kiki.NewQueue(kiki.QueueOptions{
		Redis: rdb,
		Name:  "example",
		// Backoff 用小参数让重试在示例里几秒内可见；生产默认 1s/60s/±0.5。
		// （Backoff 属于 Worker 选项，见下方 WorkerOptions。）
	})
	if err != nil {
		fatal("NewQueue", err)
	}
	defer q.Close()

	// ---- 1. 生产：四类任务 ----
	body := func(j Job) []byte { b, _ := json.Marshal(j); return b }

	if err := q.Enqueue(ctx, "email:u1:welcome", body(Job{To: "u1@kiki.dev", Tpl: "welcome"})); err != nil {
		fatal("enqueue welcome", err)
	}
	if err := q.EnqueueIn(ctx, "email:u2:digest", body(Job{To: "u2@kiki.dev", Tpl: "digest"}), 300*time.Millisecond); err != nil {
		fatal("enqueue digest", err)
	}
	// 毒丸：payload 不是合法 JSON → NonRetryable → 首投即进 DLQ。
	if err := q.Enqueue(ctx, "email:u3:bad", []byte("this is not json")); err != nil {
		fatal("enqueue bad", err)
	}
	// 瞬时故障：前 2 次投递失败，第 3 次成功 → 演示 fail→RETRY 退避重试。
	if err := q.Enqueue(ctx, "email:u4:flaky", body(Job{To: "u4@kiki.dev", Tpl: "flaky", FailTimes: 2})); err != nil {
		fatal("enqueue flaky", err)
	}
	fmt.Println("已投递 4 个任务（welcome / digest[延迟300ms] / bad[毒丸] / flaky[失败2次]）")

	// ---- 2. 消费：Worker（自动内嵌 Scheduler）----
	w := q.NewWorker(kiki.WorkerOptions{
		Concurrency: 4,
		Backoff:     kiki.ExponentialBackoff(80*time.Millisecond, 400*time.Millisecond, 0.5),
		Middleware: []kiki.Middleware{
			middlewares.Slog(slog.Default()),
		},
		Hooks: kiki.Hooks{
			OnDLQ: func(t kiki.Task, via string, cause error) {
				fmt.Printf(">>> DLQ: task=%s via=%s cause=%v\n", t.ID, via, cause)
			},
		},
	})
	w.Handle(func(ctx context.Context, t kiki.Task) error {
		var job Job
		if err := json.Unmarshal(t.Payload, &job); err != nil {
			// 坏消息重试纯属浪费：跳过剩余重试，直达 DLQ。
			return kiki.NonRetryable(fmt.Errorf("unmarshal payload: %w", err))
		}
		if job.FailTimes > 0 && t.Tries <= job.FailTimes {
			return fmt.Errorf("transient failure (attempt %d/%d)", t.Tries, job.FailTimes+1)
		}
		fmt.Printf(">>> 处理完成: task=%s tries=%d ver=%d\n", t.ID, t.Tries, t.Ver)
		return nil // nil → complete
	})
	go func() { _ = w.Run(ctx) }() // 阻塞运行；ctx 取消即优雅下线

	// ---- 3. 等待收敛：ready/sched 清空且毒丸已入死信 ----
	fmt.Println("等待队列收敛（延迟任务 promote、毒丸进 DLQ、flaky 退避重试）...")
	deadline := time.Now().Add(15 * time.Second)
	for {
		st, err := q.Stats(ctx)
		if err != nil {
			fatal("stats", err)
		}
		if st.ReadyDepth == 0 && st.SchedDepth == 0 && st.LeaseDepth == 0 && st.DLQLen == 1 {
			break
		}
		if time.Now().After(deadline) {
			fatal("收敛超时", fmt.Errorf("stats=%+v（Redis 是否可达？）", st))
		}
		time.Sleep(50 * time.Millisecond)
	}

	// ---- 4. 优雅下线 + 终态查看 ----
	fmt.Println("优雅下线：draining → 等待在途 → Release 收尾")
	shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := w.Shutdown(shutdownCtx); err != nil {
		fmt.Println("shutdown（grace 内未等完，流程仍完整）:", err)
	}
	cancel() // Run 的 ctx 取消；后续只读操作换用独立 ctx（cancelled ctx 会被引擎拒绝）

	readCtx := context.WithoutCancel(ctx)
	st, err := q.Stats(readCtx)
	if err != nil {
		fatal("stats", err)
	}
	fmt.Printf("终态: ready=%d sched=%d lease=%d dlq=%d\n",
		st.ReadyDepth, st.SchedDepth, st.LeaseDepth, st.DLQLen)
	entries, err := q.ListDLQ(readCtx, 10)
	if err != nil {
		fatal("list dlq", err)
	}
	for _, e := range entries {
		fmt.Printf("死信快照: id=%s via=%s tries=%d err=%q\n", e.ID, e.Via, e.Tries, e.Err)
	}
	fmt.Println("示例结束。用 kikictl inspect example email:u4:flaky 可看到 tries=3/ver=5 的投递痕迹。")
}

func fatal(what string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", what, err)
	os.Exit(1)
}
