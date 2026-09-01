//go:build integration

// 集成测试黄金用例 T1–T13（go-implementation.md §10.1）。
//
// 铁律：脚本正确性只在真实 Redis 上验证——miniredis 的 Lua 子集不覆盖
// TIME/效果复制语义，这里的每条用例都跑在 testcontainers 拉起的 redis:7.2。
//
// 运行：
//
//	go test -tags=integration ./integration/
//	go test -race -tags=integration -count=10 ./integration/   # flaky 检查
//
// T12（Cluster 冒烟）需要环境变量 KIKI_TEST_CLUSTER_ADDRS（见
// docker-compose.test.yml 的 tests 服务），未设置时跳过。

package integration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/garfield-dev-team/kiki"
	"github.com/garfield-dev-team/kiki/internal/metrics"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

var (
	sharedAddr string
	queueSeq   atomic.Int64
)

func TestMain(m *testing.M) {
	if os.Getenv("KIKI_CHILD") == "1" {
		// T13 的 fork 子进程：不启动容器，直连父进程给的地址。
		os.Exit(m.Run())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := tcredis.Run(ctx, "redis:7.2")
	if err != nil {
		fmt.Fprintln(os.Stderr, "testcontainers: start redis:", err)
		fmt.Fprintln(os.Stderr, "（集成测试需要 Docker；环境受限时请如实声明集成未验证）")
		os.Exit(1)
	}
	uri, err := c.ConnectionString(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "testcontainers: connection string:", err)
		os.Exit(1)
	}
	sharedAddr = strings.TrimPrefix(uri, "redis://")
	code := m.Run()
	_ = c.Terminate(context.WithoutCancel(ctx))
	os.Exit(code)
}

// ---- harness ----

func client(t *testing.T) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: sharedAddr})
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// newQueue 每条用例独占一个队列名，避免共享容器内的状态串扰。
func newQueue(t *testing.T, overrides func(*kiki.QueueOptions)) (*kiki.Queue, *redis.Client) {
	t.Helper()
	name := fmt.Sprintf("q%d", queueSeq.Add(1))
	opts := kiki.QueueOptions{
		Redis:             client(t),
		Name:              name,
		Logger:            slog.New(slog.DiscardHandler),
		MetricsRegisterer: testRegistry.forQueue(name),
	}
	if overrides != nil {
		overrides(&opts)
	}
	q, err := kiki.NewQueue(opts)
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	return q, q.Client().(*redis.Client)
}

func runScheduler(t *testing.T, q *kiki.Queue, interval time.Duration) {
	t.Helper()
	s := q.NewScheduler(kiki.SchedulerOptions{Interval: interval})
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	t.Cleanup(cancel)
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func zcard(t *testing.T, c *redis.Client, key string) int64 {
	t.Helper()
	n, err := c.ZCard(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("zcard %s: %v", key, err)
	}
	return n
}

func hget(t *testing.T, c *redis.Client, key, field string) string {
	t.Helper()
	v, err := c.HGet(context.Background(), key, field).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatalf("hget %s %s: %v", key, field, err)
	}
	return v
}

func taskKey(q *kiki.Queue, id string) string {
	return fmt.Sprintf("{qk:%s}:t:%s", q.Name(), id)
}

// ---- 黄金用例 ----

// T1 双重预订不可能：1 任务，32 goroutine 并发 Reserve(1)。
func TestT1DoubleReserveImpossible(t *testing.T) {
	q, c := newQueue(t, nil)
	ctx := context.Background()
	id := "t1-job"
	if err := q.Enqueue(ctx, id, []byte("body")); err != nil {
		t.Fatal(err)
	}
	const workers = 32
	type result struct {
		tasks []kiki.Task
		err   error
	}
	results := make(chan result, workers)
	for i := 0; i < workers; i++ {
		go func() {
			ts, err := q.Reserve(ctx, 30*time.Second, 1)
			results <- result{ts, err}
		}()
	}
	nonEmpty := 0
	for i := 0; i < workers; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("reserve: %v", r.err)
		}
		if len(r.tasks) > 0 {
			nonEmpty++
			if r.tasks[0].Tries != 1 || r.tasks[0].Ver != 1 {
				t.Fatalf("first delivery must be tries=1 ver=1, got %+v", r.tasks[0])
			}
		}
	}
	if nonEmpty != 1 {
		t.Fatalf("exactly one reserve must succeed, got %d", nonEmpty)
	}
	if zcard(t, c, "{qk:"+q.Name()+"}:ready") != 0 {
		t.Fatal("ready must be empty")
	}
}

// T2 enqueue 幂等：同 id enqueue ×2 → 第二次 ErrDup，ZCARD 仍 1。
func TestT2EnqueueIdempotent(t *testing.T) {
	q, c := newQueue(t, nil)
	ctx := context.Background()
	id := "t2-job"
	if err := q.Enqueue(ctx, id, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	err := q.Enqueue(ctx, id, []byte("v2"))
	if !errors.Is(err, kiki.ErrDup) {
		t.Fatalf("want ErrDup, got %v", err)
	}
	if zcard(t, c, "{qk:"+q.Name()+"}:ready") != 1 {
		t.Fatal("ready must still hold exactly one entry")
	}
	if got := hget(t, c, taskKey(q, id), "payload"); got != "v1" {
		t.Fatalf("payload must be the first enqueue, got %q", got)
	}
}

// T3 租约到期重投：vis 短，reserve 后不 complete，等 sweep 重投。
// 断言同 id、Tries=2；Ver=3（勘误 #7：reserve 颁发 +1、sweep 失效 +1）。
func TestT3LeaseRequeue(t *testing.T) {
	q, _ := newQueue(t, nil)
	ctx := context.Background()
	id := "t3-job"
	if err := q.Enqueue(ctx, id, []byte("body")); err != nil {
		t.Fatal(err)
	}
	first, err := q.Reserve(ctx, 200*time.Millisecond, 1)
	if err != nil || len(first) != 1 {
		t.Fatalf("first reserve: %v %v", first, err)
	}
	runScheduler(t, q, 40*time.Millisecond)
	var second []kiki.Task
	waitFor(t, 5*time.Second, "redelivery", func() bool {
		second, err = q.Reserve(ctx, 30*time.Second, 1)
		if err != nil {
			return false
		}
		return len(second) == 1
	})
	if second[0].ID != id || second[0].Tries != 2 {
		t.Fatalf("redelivered task: %+v", second[0])
	}
	if second[0].Ver != 3 {
		t.Fatalf("ver=%d want 3 (reserve+1, sweep+1, reserve+1)", second[0].Ver)
	}
}

// T4 fencing：承 T3 场景，旧 token 一律 ErrFenced。
func TestT4FencedOldToken(t *testing.T) {
	q, _ := newQueue(t, nil)
	ctx := context.Background()
	id := "t4-job"
	if err := q.Enqueue(ctx, id, []byte("body")); err != nil {
		t.Fatal(err)
	}
	old, err := q.Reserve(ctx, 150*time.Millisecond, 1)
	if err != nil || len(old) != 1 {
		t.Fatalf("reserve: %v %v", old, err)
	}
	stale := old[0]
	runScheduler(t, q, 40*time.Millisecond)
	var fresh []kiki.Task
	waitFor(t, 5*time.Second, "redelivery", func() bool {
		fresh, err = q.Reserve(ctx, 30*time.Second, 1)
		return err == nil && len(fresh) == 1
	})
	if fresh[0].Ver == stale.Ver {
		t.Fatal("fencing token must rotate across sweep")
	}
	if err := q.Complete(ctx, stale, "late result"); !errors.Is(err, kiki.ErrFenced) {
		t.Fatalf("stale complete: want ErrFenced, got %v", err)
	}
	if err := q.Heartbeat(ctx, stale, time.Second); !errors.Is(err, kiki.ErrFenced) {
		t.Fatalf("stale heartbeat: want ErrFenced, got %v", err)
	}
	if _, err := q.Fail(ctx, stale, errors.New("late"), time.Second); !errors.Is(err, kiki.ErrFenced) {
		t.Fatalf("stale fail: want ErrFenced, got %v", err)
	}
}

// T5 complete 幂等：complete 成功后同 token 再 complete → OK_DUP（nil），
// 并计入 complete_total{result=dup}。
func TestT5CompleteIdempotent(t *testing.T) {
	q, c := newQueue(t, nil)
	ctx := context.Background()
	id := "t5-job"
	if err := q.Enqueue(ctx, id, []byte("body")); err != nil {
		t.Fatal(err)
	}
	tasks, err := q.Reserve(ctx, 30*time.Second, 1)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("reserve: %v %v", tasks, err)
	}
	if err := q.Complete(ctx, tasks[0], "done"); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(ctx, tasks[0], "done again"); err != nil {
		t.Fatalf("idempotent complete: %v", err)
	}
	if got := hget(t, c, taskKey(q, id), "state"); got != "COMPLETED" {
		t.Fatalf("state=%s want COMPLETED", got)
	}
	assertCounter(t, q.Name(), "kiki_complete_total", "dup", 1)
}

// T6 fail 退避：fail(err, backoff=1s) → task 进 sched，score≈now+1s，
// state=SCHEDULED，lease 清空。
func TestT6FailBackoff(t *testing.T) {
	q, c := newQueue(t, nil)
	ctx := context.Background()
	id := "t6-job"
	if err := q.Enqueue(ctx, id, []byte("body")); err != nil {
		t.Fatal(err)
	}
	tasks, err := q.Reserve(ctx, 30*time.Second, 1)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("reserve: %v %v", tasks, err)
	}
	res, err := q.Fail(ctx, tasks[0], errors.New("boom"), time.Second)
	if err != nil || res != kiki.Retried {
		t.Fatalf("fail: res=%v err=%v", res, err)
	}
	if got := hget(t, c, taskKey(q, id), "state"); got != "SCHEDULED" {
		t.Fatalf("state=%s want SCHEDULED", got)
	}
	key := "{qk:" + q.Name() + "}:sched"
	if zcard(t, c, key) != 1 {
		t.Fatal("task must be in sched")
	}
	zs, _ := c.ZRangeWithScores(ctx, key, 0, -1).Result()
	due := int64(zs[0].Score)
	now := time.Now().Add(500 * time.Millisecond).UnixMilli() // 容忍时钟取整
	if delta := due - now; delta < 300 || delta > 1700 {
		t.Fatalf("sched score %d not ~now+1s (delta %dms)", due, delta)
	}
	if zcard(t, c, "{qk:"+q.Name()+"}:lease") != 0 {
		t.Fatal("lease must be empty after fail")
	}
	if got := hget(t, c, taskKey(q, id), "last_error"); got != "boom" {
		t.Fatalf("last_error=%q", got)
	}
}

// T7 毒丸·fail 路径：MaxRetries=2，连续三轮 reserve+fail，
// 第三次 DeadLettered；DLQ 快照含 payload/err/tries/via=fail。
// （fail 的退避要经 promote 回 ready，故需要 scheduler 在跑。）
func TestT7PoisonFailPath(t *testing.T) {
	q, c := newQueue(t, func(o *kiki.QueueOptions) { o.MaxRetries = 2 })
	runScheduler(t, q, 20*time.Millisecond)
	ctx := context.Background()
	id := "t7-job"
	if err := q.Enqueue(ctx, id, []byte("poison")); err != nil {
		t.Fatal(err)
	}
	var tasks []kiki.Task
	for round := 1; round <= 3; round++ {
		if round > 1 {
			// 上一轮 fail 的退避要经 promote 回 ready 才能再领。
			waitFor(t, 3*time.Second, fmt.Sprintf("round %d redeliverable", round), func() bool {
				ts, err := q.Reserve(ctx, time.Second, 1)
				if err != nil || len(ts) == 0 {
					return false
				}
				tasks = ts
				return true
			})
		} else {
			var err error
			tasks, err = q.Reserve(ctx, 30*time.Second, 1)
			if err != nil || len(tasks) != 1 {
				t.Fatalf("round 1 reserve: %v %v", tasks, err)
			}
		}
		if tasks[0].Tries != round {
			t.Fatalf("round %d: tries=%d", round, tasks[0].Tries)
		}
		res, ferr := q.Fail(ctx, tasks[0], fmt.Errorf("boom-%d", round), time.Millisecond)
		if round < 3 {
			if ferr != nil || res != kiki.Retried {
				t.Fatalf("round %d fail: res=%v err=%v", round, res, ferr)
			}
		} else {
			if ferr != nil || res != kiki.DeadLettered {
				t.Fatalf("round 3 fail: res=%v err=%v", res, ferr)
			}
		}
	}
	dlqKey := "{qk:" + q.Name() + "}:dlq"
	if n, _ := c.XLen(ctx, dlqKey).Result(); n != 1 {
		t.Fatalf("dlq len=%d want 1", n)
	}
	entries, err := q.ListDLQ(ctx, 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("list dlq: %v %v", entries, err)
	}
	e := entries[0]
	if e.ID != id || e.Via != "fail" || e.Tries != 3 || e.Err != "boom-3" {
		t.Fatalf("dlq snapshot: %+v", e)
	}
	if string(e.Payload) != "poison" || e.MaxRetries != 2 {
		t.Fatalf("dlq snapshot payload/maxRetries: %+v", e)
	}
	if got := hget(t, c, taskKey(q, id), "state"); got != "DLQ" {
		t.Fatalf("state=%s want DLQ", got)
	}
	assertCounter(t, q.Name(), "kiki_dlq_total", "fail", 1)
}

// T8 毒丸·sweep 路径：MaxRedeliveries=1，反复领了不 ack，
// 第二轮 sweep 后进 DLQ，err=lease_exceeded，via=sweep。
func TestT8PoisonSweepPath(t *testing.T) {
	q, c := newQueue(t, func(o *kiki.QueueOptions) { o.MaxRedeliveries = 1 })
	ctx := context.Background()
	id := "t8-job"
	if err := q.Enqueue(ctx, id, []byte("killer")); err != nil {
		t.Fatal(err)
	}
	runScheduler(t, q, 30*time.Millisecond)
	// 持续"领取、不 ack"：租约到期 → sweep 重投（lease_resets+1），
	// 直到 resets > MaxRedeliveries 汇入 DLQ。
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = q.Reserve(ctx, 100*time.Millisecond, 1) // 领了不 ack
			time.Sleep(10 * time.Millisecond)
		}
	}()
	defer close(stop)
	waitFor(t, 10*time.Second, "sweep-path DLQ", func() bool {
		n, _ := c.XLen(ctx, "{qk:"+q.Name()+"}:dlq").Result()
		return n == 1
	})
	entries, _ := q.ListDLQ(ctx, 10)
	if len(entries) != 1 || entries[0].Via != "sweep" || entries[0].Err != "lease_exceeded" {
		t.Fatalf("sweep dlq snapshot: %+v", entries)
	}
	if got := hget(t, c, taskKey(q, id), "lease_resets"); got != "2" {
		t.Fatalf("lease_resets=%q want 2", got)
	}
}

// T9 优雅下线：draining 时 1 个在途任务收到 Release，tries 不变，立即可重领。
func TestT9GracefulShutdownRelease(t *testing.T) {
	q, c := newQueue(t, nil)
	ctx := context.Background()
	id := "t9-job"
	if err := q.Enqueue(ctx, id, []byte("body")); err != nil {
		t.Fatal(err)
	}
	w := q.NewWorker(kiki.WorkerOptions{
		Concurrency:       1,
		VisibilityTimeout: 5 * time.Second,
		ShutdownGrace:     300 * time.Millisecond,
		PollInterval:      20 * time.Millisecond,
		SchedulerInterval: 0, // 本用例不内嵌 scheduler
		Logger:            slog.New(slog.DiscardHandler),
	})
	entered := make(chan struct{})
	w.Handle(func(ctx context.Context, task kiki.Task) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err() // 因 shutdown 被取消 ⇒ 应走 Release 而非 Fail
	})
	runErr := make(chan error, 1)
	go func() { runErr <- w.Run(ctx) }()
	<-entered
	// Shutdown：grace 超时后强制取消 handler ⇒ Terminator Release。
	_ = w.Shutdown(context.Background())
	// Release 的语义：不计失败——release 本身不增 tries、不 bump ver。
	// hash 上的 tries 保持领出时的 1，state=READY 立即可领。
	if got := hget(t, c, taskKey(q, id), "tries"); got != "1" {
		t.Fatalf("tries after release=%q want 1 (release must not count failure)", got)
	}
	if got := hget(t, c, taskKey(q, id), "state"); got != "READY" {
		t.Fatalf("state after release=%q want READY", got)
	}
	tasks, err := q.Reserve(ctx, 30*time.Second, 1)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("task must be immediately re-reservable after release: %v %v", tasks, err)
	}
	if tasks[0].ID != id {
		t.Fatalf("wrong task: %+v", tasks[0])
	}
	// 重新领取是一次新投递：tries 由 reserve 递增为 2；ver 未被 release
	// 毒杀，re-reserve 后 = 2（区别于 sweep 路径的 3）。
	if tasks[0].Tries != 2 || tasks[0].Ver != 2 {
		t.Fatalf("re-reserve after release: tries=%d ver=%d want 2/2", tasks[0].Tries, tasks[0].Ver)
	}
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("run err: %v", err)
		}
	default:
	}
}

// T10 延迟任务：delay=500ms，promote 前 reserve 为空，到点后可领。
func TestT10DelayedTask(t *testing.T) {
	q, c := newQueue(t, nil)
	ctx := context.Background()
	id := "t10-job"
	if err := q.EnqueueIn(ctx, id, []byte("later"), 500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got := hget(t, c, taskKey(q, id), "state"); got != "SCHEDULED" {
		t.Fatalf("state=%s want SCHEDULED", got)
	}
	if tasks, _ := q.Reserve(ctx, time.Second, 1); len(tasks) != 0 {
		t.Fatalf("must not be reservable before due: %+v", tasks)
	}
	runScheduler(t, q, 40*time.Millisecond)

	var tasks []kiki.Task
	waitFor(t, 5*time.Second, "promote+reserve", func() bool {
		tasks, _ = q.Reserve(ctx, 30*time.Second, 1)
		return len(tasks) == 1
	})
	if tasks[0].ID != id {
		t.Fatalf("wrong task: %+v", tasks[0])
	}
}

// T11 NonRetryable：handler 返回 NonRetryable → tries=1 即进 DLQ，via=abandon。
func TestT11NonRetryable(t *testing.T) {
	q, _ := newQueue(t, nil)
	ctx := context.Background()
	id := "t11-job"
	if err := q.Enqueue(ctx, id, []byte("bad")); err != nil {
		t.Fatal(err)
	}
	dlqSeen := make(chan kiki.Task, 1)
	w := q.NewWorker(kiki.WorkerOptions{
		Concurrency:       1,
		VisibilityTimeout: 5 * time.Second,
		PollInterval:      20 * time.Millisecond,
		SchedulerInterval: 0,
		Hooks: kiki.Hooks{OnDLQ: func(task kiki.Task, via string, cause error) {
			dlqSeen <- task
		}},
		Logger: slog.New(slog.DiscardHandler),
	})
	w.Handle(func(ctx context.Context, task kiki.Task) error {
		return kiki.NonRetryable(errors.New("bad payload"))
	})
	go func() { _ = w.Run(ctx) }()
	select {
	case task := <-dlqSeen:
		if task.ID != id {
			t.Fatalf("dlq task: %+v", task)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("task did not dead-letter")
	}
	entries, _ := q.ListDLQ(ctx, 10)
	if len(entries) != 1 || entries[0].Via != "abandon" || entries[0].Tries != 1 {
		t.Fatalf("abandon snapshot: %+v", entries)
	}
	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// T12 Cluster 冒烟：4-master cluster（compose），T1/T3/T7 复跑
// （hash tag 路由验证）。需要 KIKI_TEST_CLUSTER_ADDRS。
func TestT12ClusterSmoke(t *testing.T) {
	env := os.Getenv("KIKI_TEST_CLUSTER_ADDRS")
	if env == "" {
		t.Skip("KIKI_TEST_CLUSTER_ADDRS 未设置（docker compose -f docker-compose.test.yml up 启动）")
	}
	addrs := strings.Split(env, ",")
	c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("cluster ping: %v", err)
	}
	defer c.Close()
	t.Run("double_reserve", func(t *testing.T) { clusterT1(t, c) })
	t.Run("lease_requeue", func(t *testing.T) { clusterT3(t, c) })
	t.Run("poison_fail", func(t *testing.T) { clusterT7(t, c) })
}

// T13 进程崩溃：reserve 后 kill -9 子进程，租约到期重投，ver 递增，
// 无孤儿 lease 条目残留。
func TestT13ProcessCrash(t *testing.T) {
	q, c := newQueue(t, nil)
	ctx := context.Background()
	id := "t13-job"
	if err := q.Enqueue(ctx, id, []byte("body")); err != nil {
		t.Fatal(err)
	}
	// 子进程：连同一 Redis，reserve 后原地等死（父进程 kill -9）。
	childAddr := sharedAddr
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := childProcess(t, exe, childAddr, q.Name(), id)
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 256)
	n, _ := pipe.Read(buf) // 阻塞等待子进程 RESERVED 标记
	if !strings.Contains(string(buf[:n]), "RESERVED") {
		t.Fatalf("child did not report RESERVED: %q", string(buf[:n]))
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	leaseKey := "{qk:" + q.Name() + "}:lease"
	// 租约到期前：lease 条目仍在（子进程领取过，vis=5s）。
	// 触发 sweep 后：条目被清掉、任务重投、ver 递增。
	waitFor(t, 15*time.Second, "lease entry to expire", func() bool {
		dl, err := c.ZScore(ctx, leaseKey, id).Result()
		return err != nil || dl < float64(time.Now().UnixMilli())
	})
	res, err := q.SweepOnce(ctx, 200)
	if err != nil {
		t.Fatal(err)
	}
	if res.Requeued != 1 {
		t.Fatalf("sweep requeued=%d want 1", res.Requeued)
	}
	if zcard(t, c, leaseKey) != 0 {
		t.Fatal("orphan lease entry must be removed by sweep")
	}
	tasks, err := q.Reserve(ctx, 30*time.Second, 1)
	if err != nil || len(tasks) != 1 || tasks[0].ID != id {
		t.Fatalf("redelivery after crash: %v %v", tasks, err)
	}
	if tasks[0].Tries != 2 || tasks[0].Ver < 3 {
		t.Fatalf("after crash redelivery: tries=%d ver=%d", tasks[0].Tries, tasks[0].Ver)
	}
}

// TestT13ChildHelper 是 T13 的 fork 目标；只在 KIKI_CHILD=1 时执行。
func TestT13ChildHelper(t *testing.T) {
	if os.Getenv("KIKI_CHILD") != "1" {
		t.Skip("child only")
	}
	addr := os.Getenv("KIKI_TEST_ADDR")
	qname := os.Getenv("KIKI_TEST_QUEUE")
	id := os.Getenv("KIKI_TEST_TASK")
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer c.Close()
	q, err := kiki.NewQueue(kiki.QueueOptions{
		Redis:  c,
		Name:   qname,
		Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "child NewQueue:", err)
		os.Exit(3)
	}
	tasks, err := q.Reserve(context.Background(), 5*time.Second, 1)
	if err != nil || len(tasks) != 1 || tasks[0].ID != id {
		fmt.Fprintf(os.Stderr, "child reserve: %v %v\n", tasks, err)
		os.Exit(4)
	}
	fmt.Printf("RESERVED ver=%d\n", tasks[0].Ver) // 父进程读此标记后 kill -9
	select {}                                     // 原地等死
}

var _ = strconv.Itoa
var _ = metrics.NewNoop
