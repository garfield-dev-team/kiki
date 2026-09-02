// Package demo 是考核示例（example/*）共用的脚手架：连接、断言、等待。
// 独立成包是为了让每个示例 main 保持极短、聚焦各自的考核点。
package demo

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/garfield-dev-team/kiki"
)

// Unique 生成带时间戳后缀的名称：重复运行互不干扰；多队列场景共享同一后缀便于关联。
func Unique(base string) string {
	return fmt.Sprintf("%s-%d", base, time.Now().UnixNano()%1_000_000)
}

// Connect 按 REDIS_ADDR（默认 127.0.0.1:6379）建立连接。
func Connect() redis.UniversalClient {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	return redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{addr}})
}

// NewQueue 用共享 client 建队列（多队列场景），o 可定制队列级选项。
func NewQueue(client redis.UniversalClient, name string, o func(*kiki.QueueOptions)) *kiki.Queue {
	opts := kiki.QueueOptions{Redis: client, Name: name, Logger: slog.New(slog.DiscardHandler)}
	if o != nil {
		o(&opts)
	}
	q, err := kiki.NewQueue(opts)
	if err != nil {
		Fatal("NewQueue "+name, err)
	}
	return q
}

// Setup 连接 Redis 并创建本示例专属队列（单队列示例的快捷方式）。
func Setup(name string) (*kiki.Queue, redis.UniversalClient) {
	client := Connect()
	q := NewQueue(client, Unique(name), nil)
	fmt.Printf("队列: %s\n", q.Name())
	return q, client
}

// Fatal 打印失败原因并以非零码退出（演示版 testing）。
func Fatal(what string, err error) {
	fmt.Fprintf(os.Stderr, "✗ %s: %v\n", what, err)
	os.Exit(1)
}

// Check 断言：通过打 ✓，失败打 ✗ 并退出。
func Check(ok bool, format string, args ...any) {
	if ok {
		fmt.Printf("✓ %s\n", fmt.Sprintf(format, args...))
		return
	}
	fmt.Fprintf(os.Stderr, "✗ %s\n", fmt.Sprintf(format, args...))
	os.Exit(1)
}

// Await 轮询等待条件成立，超时退出。
func Await(what string, timeout time.Duration, cond func() bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	Fatal("等待 "+what, fmt.Errorf("超时（%v）——Redis 是否可达？scheduler 是否在跑？", timeout))
}

// RunScheduler 起一个高频 Scheduler，让 promote/sweep 在演示里秒级可见。
// 返回 stop 函数（defer 调用）。
func RunScheduler(q *kiki.Queue) (stop func()) {
	s := q.NewScheduler(kiki.SchedulerOptions{Interval: 30 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	return cancel
}

// Field 读任务 hash 的一个字段（演示只读直查；写路径永远走 SDK）。
func Field(q *kiki.Queue, c redis.UniversalClient, id, field string) string {
	v, err := c.HGet(context.Background(),
		fmt.Sprintf("{qk:%s}:t:%s", q.Name(), id), field).Result()
	if err != nil {
		return "" // redis.Nil 等
	}
	return v
}

// State 是 Field(q, c, id, "state") 的快捷方式。
func State(q *kiki.Queue, c redis.UniversalClient, id string) string {
	return Field(q, c, id, "state")
}
