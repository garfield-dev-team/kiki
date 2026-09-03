//go:build integration

// 基准（go-implementation.md §10.2）：单机 Redis 期望 reserve+complete ≥ 8k ops/s。
// 运行：go test -tags=integration -bench=. -benchtime=3s -run=^$ ./integration/
package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/garfield-dev-team/kiki"
)

func BenchmarkReserveComplete(b *testing.B) {
	q, err := kiki.NewQueue(kiki.QueueOptions{
		Redis:  client(&testing.T{}),
		Name:   fmt.Sprintf("bench%d", time.Now().UnixNano()%1_000_000),
		Logger: discardLogger(),
	})
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()
	ctx := context.Background()

	// 预灌 b.N 个任务，基准体只测 reserve+complete 的往返成本。
	tasks := make([]kiki.Task, b.N)
	for i := range tasks {
		tasks[i] = kiki.Task{ID: fmt.Sprintf("b-%08d", i), Payload: []byte("x")}
	}
	if err := q.EnqueueBulk(ctx, tasks); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts, err := q.Reserve(ctx, 30*time.Second, 1)
		if err != nil {
			b.Fatal(err)
		}
		if len(ts) != 1 {
			b.Fatal("queue drained early")
		}
		if err := q.Complete(ctx, ts[0], ""); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkShardedReserveCompleteParallel（docs/sharded-queue.md §10.3 验收基准）：
// N 分片在多 master 集群上的并发吞吐形态，对照 BenchmarkReserveCompleteParallel
// （单队列 = 单 slot = 单 master）。需要 KIKI_TEST_CLUSTER_ADDRS（compose 集群），
// 未设置时跳过——分片扩展性只有在任务真散布到多个 master 上才可测。
// 路由本身只是 fnv 取模；reserve 在积压下首探测即命中，开销与单队列同量级。
func BenchmarkShardedReserveCompleteParallel(b *testing.B) {
	env := os.Getenv("KIKI_TEST_CLUSTER_ADDRS")
	if env == "" {
		b.Skip("KIKI_TEST_CLUSTER_ADDRS 未设置（docker compose -f docker-compose.test.yml up 启动）")
	}
	c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(env, ",")})
	if err := c.Ping(context.Background()).Err(); err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	sq, err := kiki.NewShardedQueue(kiki.ShardedOptions{
		Redis:  c,
		Name:   fmt.Sprintf("bsq%d", time.Now().UnixNano()%1_000_000),
		Shards: 4,
		Logger: discardLogger(),
	})
	if err != nil {
		b.Fatal(err)
	}
	defer sq.Close()
	ctx := context.Background()
	tasks := make([]kiki.Task, b.N)
	for i := range tasks {
		tasks[i] = kiki.Task{ID: fmt.Sprintf("bs-%08d", i), Payload: []byte("x")}
	}
	if err := sq.EnqueueBulk(ctx, tasks); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ts, err := sq.Reserve(ctx, 30*time.Second, 1)
			if err != nil {
				b.Fatal(err)
			}
			if len(ts) != 1 {
				b.Fatal("queue drained early")
			}
			if err := sq.Complete(ctx, ts[0], ""); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkEnqueue(b *testing.B) {
	q, err := kiki.NewQueue(kiki.QueueOptions{
		Redis:  client(&testing.T{}),
		Name:   fmt.Sprintf("benqe%d", time.Now().UnixNano()%1_000_000),
		Logger: discardLogger(),
	})
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := q.Enqueue(ctx, fmt.Sprintf("e-%08d", i), []byte("x")); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReserveCompleteParallel 是并发槽位下的吞吐形态：
// 吞吐随 Concurrency 线性扩展，直至 Redis 单分片 RTT 上限（§9）。
func BenchmarkReserveCompleteParallel(b *testing.B) {
	q, err := kiki.NewQueue(kiki.QueueOptions{
		Redis:  client(&testing.T{}),
		Name:   fmt.Sprintf("benchp%d", time.Now().UnixNano()%1_000_000),
		Logger: discardLogger(),
	})
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()
	ctx := context.Background()
	tasks := make([]kiki.Task, b.N)
	for i := range tasks {
		tasks[i] = kiki.Task{ID: fmt.Sprintf("bp-%08d", i), Payload: []byte("x")}
	}
	if err := q.EnqueueBulk(ctx, tasks); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ts, err := q.Reserve(ctx, 30*time.Second, 1)
			if err != nil {
				b.Fatal(err)
			}
			if len(ts) != 1 {
				b.Fatal("queue drained early")
			}
			if err := q.Complete(ctx, ts[0], ""); err != nil {
				b.Fatal(err)
			}
		}
	})
}
