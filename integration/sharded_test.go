//go:build integration

// ShardedQueue 集成用例 T14–T17 + 等价性矩阵（docs/sharded-queue.md §11）。
//
// 等价性矩阵把 v1 黄金用例的核心不变式逐字复刻在 ShardedQueue(N=4) 上：
// 路由句柄、manifest 契约都是新代码，但状态机语义必须与单队列逐字一致。
// 与 T1–T13 一样，只在真实 Redis 上运行（miniredis 不覆盖脚本语义）。
//
// 运行：
//
//	go test -tags=integration ./integration/
//	go test -race -tags=integration -count=10 ./integration/   # flaky 检查
package integration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/garfield-dev-team/kiki"
)

// ---- harness ----

func newSharded(t *testing.T, overrides func(*kiki.ShardedOptions)) *kiki.ShardedQueue {
	t.Helper()
	opts := kiki.ShardedOptions{
		Redis:  client(t),
		Name:   fmt.Sprintf("sq%d", queueSeq.Add(1)),
		Shards: 4,
		Logger: discardLogger(),
	}
	if overrides != nil {
		overrides(&opts)
	}
	sq, err := kiki.NewShardedQueue(opts)
	if err != nil {
		t.Fatalf("NewShardedQueue: %v", err)
	}
	return sq
}

func shardKey(name string, k int, suffix string) string {
	return fmt.Sprintf("{qk:%s#%d}:%s", name, k, suffix)
}

func shardTaskKey(name string, k int, id string) string {
	return fmt.Sprintf("{qk:%s#%d}:t:%s", name, k, id)
}

func exists(t *testing.T, c *redis.Client, key string) bool {
	t.Helper()
	n, err := c.Exists(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("exists %s: %v", key, err)
	}
	return n == 1
}

// ---- T14 路由与分布 ----

func TestT14ShardedRouting(t *testing.T) {
	c := client(t)
	sq := newSharded(t, nil)
	ctx := context.Background()
	n := sq.Shards()

	// 默认路由：任务 hash 只存在于 ShardOf(id) 预测的分片。
	for i := 0; i < 40; i++ {
		id := fmt.Sprintf("t14-%03d", i)
		if err := sq.Enqueue(ctx, id, []byte("body")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 40; i++ {
		id := fmt.Sprintf("t14-%03d", i)
		want := kiki.ShardOf(id, n)
		if !exists(t, c, shardTaskKey(sq.Name(), want, id)) {
			t.Fatalf("%s must live in shard %d", id, want)
		}
		for k := 0; k < n; k++ {
			if k != want && exists(t, c, shardTaskKey(sq.Name(), k, id)) {
				t.Fatalf("%s must not exist in shard %d", id, k)
			}
		}
	}

	// WithRouteKey：同一路由键恒落同一分片（与 id 无关）。
	tenant42 := kiki.ShardOf("tenant-42", n)
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("t14rk-a-%02d", i)
		if err := sq.Enqueue(ctx, id, []byte("body"), kiki.WithRouteKey("tenant-42")); err != nil {
			t.Fatal(err)
		}
		if !exists(t, c, shardTaskKey(sq.Name(), tenant42, id)) {
			t.Fatalf("%s (route tenant-42) must live in shard %d", id, tenant42)
		}
	}
	tenant7 := kiki.ShardOf("tenant-7", n)
	if err := sq.Enqueue(ctx, "t14rk-b-00", []byte("body"), kiki.WithRouteKey("tenant-7")); err != nil {
		t.Fatal(err)
	}
	if !exists(t, c, shardTaskKey(sq.Name(), tenant7, "t14rk-b-00")) {
		t.Fatalf("route tenant-7 must live in shard %d", tenant7)
	}

	// 幂等：同 id 重复入队 → ErrDup（分片内 NX 语义）。
	if err := sq.Enqueue(ctx, "t14-000", []byte("v2")); !errors.Is(err, kiki.ErrDup) {
		t.Fatalf("want ErrDup, got %v", err)
	}

	// EnqueueBulk 按条路由：全部落在预测分片。
	var bulk []kiki.Task
	for i := 0; i < 20; i++ {
		tsk, err := kiki.NewJSONTask(fmt.Sprintf("t14bulk-%02d", i), map[string]int{"i": i})
		if err != nil {
			t.Fatal(err)
		}
		bulk = append(bulk, tsk)
	}
	if err := sq.EnqueueBulk(ctx, bulk); err != nil {
		t.Fatal(err)
	}
	for _, tsk := range bulk {
		if !exists(t, c, shardTaskKey(sq.Name(), kiki.ShardOf(tsk.ID, n), tsk.ID)) {
			t.Fatalf("bulk %s must live in shard %d", tsk.ID, kiki.ShardOf(tsk.ID, n))
		}
	}
}

// ---- T15 跨分片并发恰好一次 ----

func TestT15ShardedConcurrentReserve(t *testing.T) {
	sq := newSharded(t, nil)
	ctx := context.Background()
	const total = 400
	const consumers = 32
	for i := 0; i < total; i++ {
		if err := sq.Enqueue(ctx, fmt.Sprintf("t15-%03d", i), []byte("body")); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	got := make(map[string]int)
	var wg sync.WaitGroup
	for g := 0; g < consumers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			worker := fmt.Sprintf("t15-worker-%d", g)
			for {
				ts, err := sq.ReserveFor(ctx, worker, 30*time.Second, 1)
				if err != nil {
					t.Errorf("reserve: %v", err)
					return
				}
				if len(ts) == 0 {
					return
				}
				mu.Lock()
				got[ts[0].ID]++
				mu.Unlock()
				// 路由句柄必须与路由函数一致：终结写靠它回原分片。
				if want := kiki.ShardOf(ts[0].ID, sq.Shards()); ts[0].Shard != want {
					t.Errorf("task %s: Shard=%d, want %d", ts[0].ID, ts[0].Shard, want)
				}
			}
		}(g)
	}
	wg.Wait()
	if len(got) != total {
		t.Fatalf("reserved %d/%d unique tasks", len(got), total)
	}
	for id, cnt := range got {
		if cnt != 1 {
			t.Fatalf("task %s reserved %d times — must be exactly once", id, cnt)
		}
	}
}

// ---- T16 manifest 治理（N 扩容 runbook / 缩容守卫）----

func TestT16ManifestGovernance(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	name := fmt.Sprintf("sqm%d", queueSeq.Add(1))

	// 初建：manifest 缺失 → 以本地 N 创建。
	n, ok, err := kiki.LookupManifest(ctx, c, name)
	if err != nil || ok {
		t.Fatalf("fresh name must have no manifest: n=%d ok=%v err=%v", n, ok, err)
	}
	sq2 := newSharded(t, func(o *kiki.ShardedOptions) { o.Name = name; o.Shards = 2 })

	// Strict：本地 N 漂移 → 拒绝启动，错误带修复指引。
	if _, err := newShardedErr(t, name, 4); err == nil || !strings.Contains(err.Error(), "manifest mismatch") {
		t.Fatalf("strict mismatch must refuse startup, got %v", err)
	}
	// Warn：漂移放行。
	if _, err := kiki.NewShardedQueue(kiki.ShardedOptions{
		Redis: c, Name: name, Shards: 8, ManifestCheck: kiki.ManifestWarn, Logger: discardLogger(),
	}); err != nil {
		t.Fatalf("warn mismatch must pass: %v", err)
	}
	// 一致 → 通过（幂等构造）。
	if _, err := kiki.NewShardedQueue(kiki.ShardedOptions{Redis: c, Name: name, Shards: 2, Logger: discardLogger()}); err != nil {
		t.Fatalf("consistent handshake must pass: %v", err)
	}

	// 扩容 2 → 4：SetManifest 放行；旧 N 被 Strict 拒绝；新分片接收新任务。
	if err := kiki.SetManifest(ctx, c, name, 4, false); err != nil {
		t.Fatalf("grow manifest: %v", err)
	}
	if _, err := newShardedErr(t, name, 2); err == nil {
		t.Fatal("old-N consumer must be refused after manifest bump")
	}
	sq4 := newSharded(t, func(o *kiki.ShardedOptions) { o.Name = name; o.Shards = 4 })
	newShardTasks := 0
	for i := 0; i < 40; i++ {
		id := fmt.Sprintf("t16-grow-%02d", i)
		if err := sq4.Enqueue(ctx, id, []byte("body")); err != nil {
			t.Fatal(err)
		}
		if k := kiki.ShardOf(id, 4); k >= 2 {
			newShardTasks++
			if !exists(t, c, shardTaskKey(name, k, id)) {
				t.Fatalf("%s must live in new shard %d", id, k)
			}
		}
	}
	if newShardTasks == 0 {
		t.Fatal("grow must route some tasks to new shards")
	}
	_ = sq2

	// 缩容守卫：新分片仍有任务 ⇒ 拒绝；force ⇒ 放行（孤儿任务显式承担）。
	if err := kiki.SetManifest(ctx, c, name, 2, false); err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("shrink with non-empty shards must be refused, got %v", err)
	}
	if err := kiki.SetManifest(ctx, c, name, 2, true); err != nil {
		t.Fatalf("shrink with force: %v", err)
	}
	if _, err := newShardedErr(t, name, 4); err == nil {
		t.Fatal("post-shrink N=4 must be refused")
	}
}

// newShardedErr 只关心构造错误（与 t.Fatal 解耦）。
func newShardedErr(t *testing.T, name string, shards int) (*kiki.ShardedQueue, error) {
	t.Helper()
	return kiki.NewShardedQueue(kiki.ShardedOptions{Redis: client(t), Name: name, Shards: shards, Logger: discardLogger()})
}

// ---- T17 运维面聚合（Stats / ListDLQ / ReplayDLQ / SweepOnce）----

func TestT17ShardedOpsAggregation(t *testing.T) {
	c := client(t)
	sq := newSharded(t, nil)
	ctx := context.Background()
	name := sq.Name()
	n := sq.Shards()

	// 即投 8 + 延迟 4：聚合深度必须等于分片明细之和。
	for i := 0; i < 8; i++ {
		if err := sq.Enqueue(ctx, fmt.Sprintf("t17-%02d", i), []byte("body")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		if err := sq.EnqueueIn(ctx, fmt.Sprintf("t17d-%02d", i), []byte("body"), 300*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	st, err := sq.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.ReadyDepth != 8 || st.SchedDepth != 4 {
		t.Fatalf("agg stats: %+v", st.Stats)
	}
	if len(st.Shards) != n {
		t.Fatalf("per-shard detail len=%d, want %d", len(st.Shards), n)
	}
	var sumReady, sumSched int64
	for k := 0; k < n; k++ {
		sumReady += zcard(t, c, shardKey(name, k, "ready"))
		sumSched += zcard(t, c, shardKey(name, k, "sched"))
	}
	if sumReady != st.ReadyDepth || sumSched != st.SchedDepth {
		t.Fatalf("aggregate != Σshards: ready %d/%d sched %d/%d", st.ReadyDepth, sumReady, st.SchedDepth, sumSched)
	}

	// SweepOnce 促进延迟任务（幂等无主，扇出全分片）。
	waitFor(t, 5*time.Second, "delayed tasks to become visible", func() bool {
		time.Sleep(300 * time.Millisecond)
		if _, err := sq.SweepOnce(ctx, 200); err != nil {
			t.Fatal(err)
		}
		return zcard(t, c, shardKey(name, 0, "sched"))+
			zcard(t, c, shardKey(name, 1, "sched"))+
			zcard(t, c, shardKey(name, 2, "sched"))+
			zcard(t, c, shardKey(name, 3, "sched")) == 0
	})

	// 毒丸 → 各分片 DLQ：WithMaxRetries(1) ⇒ 第 2 次 fail 死信。
	poison := []string{}
	for i := 0; len(poison) < 4 && i < 100; i++ {
		id := fmt.Sprintf("t17p-%02d", i)
		if kiki.ShardOf(id, n) == len(poison) { // 恰好第 len(poison) 个分片
			poison = append(poison, id)
		}
	}
	if len(poison) < 4 {
		t.Fatal("could not find ids covering 4 shards")
	}
	for _, id := range poison {
		if err := sq.Enqueue(ctx, id, []byte("poison"), kiki.WithMaxRetries(1)); err != nil {
			t.Fatal(err)
		}
	}
	// 收敛循环：只有毒丸被 Fail（普通任务立即 Release，不消耗 tries），
	// 直到 4 个毒丸全部死信——它们分属 4 个不同分片。
	isPoison := map[string]bool{}
	for _, id := range poison {
		isPoison[id] = true
	}
	waitFor(t, 15*time.Second, "all four poison tasks to dead-letter", func() bool {
		entries, err := sq.ListDLQ(ctx, 50)
		if err != nil {
			t.Fatal(err)
		}
		dlq := map[string]bool{}
		for _, e := range entries {
			dlq[e.ID] = true
		}
		missing := 0
		for _, id := range poison {
			if !dlq[id] {
				missing++
			}
		}
		if missing == 0 {
			return true
		}
		ts, err := sq.Reserve(ctx, 30*time.Second, 1)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case len(ts) == 0:
			time.Sleep(5 * time.Millisecond)
		case isPoison[ts[0].ID]:
			// 第 1 次 fail → Retried；第 2 次（tries 超 MaxRetries）→ DeadLettered。
			// 两种都合法，收敛以 DLQ 完整性为准。
			if _, ferr := sq.Fail(ctx, ts[0], errors.New("boom"), time.Millisecond); ferr != nil {
				t.Fatal(ferr)
			}
		default:
			if err := sq.Release(ctx, ts[0]); err != nil {
				t.Fatal(err)
			}
		}
		// promote 重试退避中的毒丸（无内嵌 scheduler 的裸引擎路径）。
		if _, err := sq.SweepOnce(ctx, 200); err != nil {
			t.Fatal(err)
		}
		return false
	})
	entries, err := sq.ListDLQ(ctx, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("merged DLQ len=%d, want 4", len(entries))
	}
	for _, e := range entries {
		if e.Via != "fail" {
			t.Fatalf("entry %s via=%s, want fail", e.ID, e.Via)
		}
		if e.Shard != kiki.ShardOf(e.ID, n) {
			t.Fatalf("entry %s Shard=%d, want %d", e.ID, e.Shard, kiki.ShardOf(e.ID, n))
		}
	}

	// 聚合 DLQ 深度 = Σ分片。
	st2, err := sq.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var sumDLQ int64
	for k := 0; k < n; k++ {
		ln, err := c.XLen(ctx, shardKey(name, k, "dlq")).Result()
		if err != nil {
			t.Fatal(err)
		}
		sumDLQ += ln
	}
	if st2.DLQLen != sumDLQ {
		t.Fatalf("dlq aggregate %d != Σshards %d", st2.DLQLen, sumDLQ)
	}

	// ReplayDLQ 按 Shard 句柄回原分片（残留 hash 在 DLQ 态需 force 原子清除）。
	if err := sq.ReplayDLQ(ctx, entries[:2], true); err != nil {
		t.Fatal(err)
	}
	for _, e := range entries[:2] {
		if !exists(t, c, shardTaskKey(name, e.Shard, e.ID)) {
			t.Fatalf("replayed %s must live back in shard %d", e.ID, e.Shard)
		}
		ready := zcard(t, c, shardKey(name, e.Shard, "ready"))
		if ready == 0 {
			t.Fatalf("replayed %s must sit in shard %d ready", e.ID, e.Shard)
		}
	}
	st3, err := sq.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// 回放不删 DLQ Stream 条目——Stream 是审计日志（勘误 #6 的语义：脚本只
	// DEL 残留 hash 并重投），条目由 XTRIM 按上限收敛，故仍为 4。
	if st3.DLQLen != 4 {
		t.Fatalf("replay keeps stream entries for audit, dlq=%d, want 4", st3.DLQLen)
	}
}

// ---- 等价性矩阵：v1 黄金用例核心不变式在 ShardedQueue 上的逐字复刻 ----

// EQ-T1 双重预订不可能。
func TestShardedEQDoubleReserve(t *testing.T) {
	sq := newSharded(t, nil)
	ctx := context.Background()
	if err := sq.Enqueue(ctx, "eqt1", []byte("body")); err != nil {
		t.Fatal(err)
	}
	const workers = 32
	results := make(chan []kiki.Task, workers)
	for i := 0; i < workers; i++ {
		go func() {
			ts, err := sq.Reserve(ctx, 30*time.Second, 1)
			if err != nil {
				t.Errorf("reserve: %v", err)
				return
			}
			results <- ts
		}()
	}
	nonEmpty := 0
	for i := 0; i < workers; i++ {
		ts := <-results
		if len(ts) > 0 {
			nonEmpty++
			if ts[0].Tries != 1 || ts[0].Ver != 1 {
				t.Fatalf("first delivery must be tries=1 ver=1, got %+v", ts[0])
			}
		}
	}
	if nonEmpty != 1 {
		t.Fatalf("exactly one reserve must succeed, got %d", nonEmpty)
	}
}

// EQ-T2 入队幂等 + EQ-T5 complete 幂等 + EQ-T4 fence 旧令牌。
func TestTShardedEQFenceAndIdempotency(t *testing.T) {
	sq := newSharded(t, nil)
	ctx := context.Background()
	if err := sq.Enqueue(ctx, "eqt4", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := sq.Enqueue(ctx, "eqt4", []byte("v2")); !errors.Is(err, kiki.ErrDup) {
		t.Fatalf("want ErrDup, got %v", err)
	}
	ts, err := sq.Reserve(ctx, 80*time.Millisecond, 1)
	if err != nil || len(ts) != 1 {
		t.Fatalf("reserve: %v %v", ts, err)
	}
	t1 := ts[0]
	time.Sleep(150 * time.Millisecond) // vis 过期
	// sweep 重投（ver+1）→ reserve 颁发（ver+1）：新令牌 ver=3。
	if _, err := sq.SweepOnce(ctx, 200); err != nil {
		t.Fatal(err)
	}
	ts2, err := sq.Reserve(ctx, 30*time.Second, 1)
	if err != nil || len(ts2) != 1 {
		t.Fatalf("re-reserve after sweep: %v %v", ts2, err)
	}
	t2 := ts2[0]
	if t2.Tries != 2 || t2.Ver != 3 {
		t.Fatalf("redelivery must be tries=2 ver=3 (sweep+reserve), got %+v", t2)
	}
	// 旧令牌的一切终结写被拒 ⇒ ErrFenced（fencing 不变式跨分片句柄成立）。
	if err := sq.Complete(ctx, t1, "late"); !errors.Is(err, kiki.ErrFenced) {
		t.Fatalf("old-token complete: want ErrFenced, got %v", err)
	}
	if _, err := sq.Fail(ctx, t1, errors.New("late"), 0); !errors.Is(err, kiki.ErrFenced) {
		t.Fatalf("old-token fail: want ErrFenced, got %v", err)
	}
	if err := sq.Heartbeat(ctx, t1, time.Second); !errors.Is(err, kiki.ErrFenced) {
		t.Fatalf("old-token heartbeat: want ErrFenced, got %v", err)
	}
	// 新令牌 complete 幂等：首写 OK，重写（响应丢失重试）OK_DUP 亦为 nil。
	if err := sq.Complete(ctx, t2, "done"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := sq.Complete(ctx, t2, "done"); err != nil {
		t.Fatalf("idempotent complete must be nil, got %v", err)
	}
}

// EQ-T3 租约到期 sweep 重投。T3 的构造语义是"reserve 后不 complete"——
// Worker 的 Terminator 在 handler 返回 nil 时立即 complete，任务进终态、
// 租约消失，sweep 无事可做；所以这里走裸引擎路径（与单队列 T3 逐字对应），
// Worker 内嵌 scheduler 的行为由下面的 EQ 延迟任务用例验证。
func TestShardedEQLeaseRequeue(t *testing.T) {
	sq := newSharded(t, nil)
	ctx := context.Background()
	if err := sq.Enqueue(ctx, "eqt3", []byte("body")); err != nil {
		t.Fatal(err)
	}
	ts1, err := sq.Reserve(ctx, 60*time.Millisecond, 1)
	if err != nil || len(ts1) != 1 {
		t.Fatalf("reserve: %v %v", ts1, err)
	}
	if ts1[0].Tries != 1 || ts1[0].Ver != 1 {
		t.Fatalf("first delivery: %+v", ts1[0])
	}
	time.Sleep(100 * time.Millisecond) // 租约过期
	if _, err := sq.SweepOnce(ctx, 200); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, "sweep redelivery", func() bool {
		got, err := sq.Reserve(ctx, time.Second, 1)
		if err != nil || len(got) == 0 {
			return false
		}
		// ver 不变式：第 2 次投递 ver = 2×2−1 = 3（sweep 失效 +1、reserve 颁发 +1）。
		if got[0].Tries != 2 || got[0].Ver != 3 {
			t.Fatalf("redelivery invariants: %+v", got[0])
		}
		if got[0].Shard != kiki.ShardOf("eqt3", sq.Shards()) {
			t.Fatalf("redelivered handle drifted: %+v", got[0])
		}
		return true
	})
}

// EQ-T3b Worker 首个实例内嵌的全分片 scheduler：promote 延迟任务并投递成功。
func TestTShardedEQEmbeddedScheduler(t *testing.T) {
	sq := newSharded(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sq.EnqueueIn(ctx, "eqt3b", []byte("body"), 150*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	var delivered atomic.Int64
	w := sq.NewWorker(kiki.WorkerOptions{
		Concurrency:       1,
		PollInterval:      5 * time.Millisecond,
		ShutdownGrace:     2 * time.Second,
		HeartbeatInterval: kiki.HeartbeatDisabled,
		SchedulerInterval: 20 * time.Millisecond, // 内嵌全分片 scheduler：promote 由它驱动
	})
	w.Handle(func(ctx context.Context, task kiki.Task) error {
		delivered.Add(1)
		return nil
	})
	done := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(done)
	}()
	waitFor(t, 10*time.Second, "delayed task promoted and delivered", func() bool {
		return delivered.Load() == 1
	})
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not shut down")
	}
}

// EQ-T6/T7 fail 路径毒丸：tries 超过 MaxRetries → DLQ。
func TestShardedEQPoisonFailPath(t *testing.T) {
	sq := newSharded(t, func(o *kiki.ShardedOptions) { o.QueueOptions.MaxRetries = 2 })
	ctx := context.Background()
	if err := sq.Enqueue(ctx, "eqt7", []byte("poison")); err != nil {
		t.Fatal(err)
	}
	for round := 1; round <= 3; round++ {
		var ts []kiki.Task
		waitFor(t, 3*time.Second, fmt.Sprintf("round %d reserve", round), func() bool {
			got, err := sq.Reserve(ctx, time.Second, 1)
			if err != nil || len(got) == 0 {
				return false
			}
			ts = got
			return true
		})
		res, ferr := sq.Fail(ctx, ts[0], fmt.Errorf("boom-%d", round), time.Millisecond)
		if round < 3 && (ferr != nil || res != kiki.Retried) {
			t.Fatalf("round %d: %v %v", round, res, ferr)
		}
		if round == 3 && (ferr != nil || res != kiki.DeadLettered) {
			t.Fatalf("round 3 must dead-letter, got %v %v", res, ferr)
		}
		if round < 3 {
			// 退避 1ms 后需要 promote 才能再次 ready（无内嵌 scheduler 的裸引擎路径）。
			time.Sleep(10 * time.Millisecond)
			if _, err := sq.SweepOnce(ctx, 200); err != nil {
				t.Fatal(err)
			}
		}
	}
	entries, err := sq.ListDLQ(ctx, 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("dlq: %v %v", entries, err)
	}
	if entries[0].Via != "fail" || entries[0].Shard != kiki.ShardOf("eqt7", sq.Shards()) {
		t.Fatalf("poison entry: %+v", entries[0])
	}
}

// EQ-T8 sweep 路径毒丸：lease_resets 超过 MaxRedeliveries → DLQ。
func TestShardedEQPoisonSweepPath(t *testing.T) {
	sq := newSharded(t, func(o *kiki.ShardedOptions) { o.QueueOptions.MaxRedeliveries = 2 })
	ctx := context.Background()
	if err := sq.Enqueue(ctx, "eqt8", []byte("crash-on-start")); err != nil {
		t.Fatal(err)
	}
	// reserve 后不处理、不心跳 ⇒ 每轮 sweep 计一次 lease_resets；第 3 次死信。
	for round := 0; round < 6; round++ {
		_, err := sq.Reserve(ctx, 30*time.Millisecond, 1)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(40 * time.Millisecond)
		if _, err := sq.SweepOnce(ctx, 200); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := sq.ListDLQ(ctx, 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("sweep-path poison must dead-letter, got %v %v", entries, err)
	}
	if entries[0].Via != "sweep" || entries[0].Shard != kiki.ShardOf("eqt8", sq.Shards()) {
		t.Fatalf("poison entry: %+v", entries[0])
	}
}

// EQ-T9 优雅 Release：tries 不变、立即可被重领（重领 tries=2 / ver=2）。
func TestTShardedEQRelease(t *testing.T) {
	sq := newSharded(t, nil)
	ctx := context.Background()
	if err := sq.Enqueue(ctx, "eqt9", []byte("body")); err != nil {
		t.Fatal(err)
	}
	ts, err := sq.Reserve(ctx, 30*time.Second, 1)
	if err != nil || len(ts) != 1 {
		t.Fatalf("reserve: %v %v", ts, err)
	}
	if err := sq.Release(ctx, ts[0]); err != nil {
		t.Fatalf("release: %v", err)
	}
	waitFor(t, 2*time.Second, "released task to be re-reservable", func() bool {
		got, err := sq.Reserve(ctx, time.Second, 1)
		if err != nil || len(got) == 0 {
			return false
		}
		if got[0].Tries != 2 || got[0].Ver != 2 {
			t.Fatalf("re-reserve after release must be tries=2 ver=2, got %+v", got[0])
		}
		return true
	})
}

// EQ-T10 延迟投递（延迟任务与重试退避共用 sched ZSET）。
func TestTShardedEQDelayed(t *testing.T) {
	sq := newSharded(t, nil)
	ctx := context.Background()
	if err := sq.EnqueueIn(ctx, "eqt10", []byte("body"), 200*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	ts, err := sq.Reserve(ctx, time.Second, 1)
	if err != nil || len(ts) != 0 {
		t.Fatalf("delayed task must not be ready yet: %v %v", ts, err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, err := sq.SweepOnce(ctx, 200); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, "delayed task to become ready", func() bool {
		got, err := sq.Reserve(ctx, time.Second, 1)
		return err == nil && len(got) == 1
	})
}

// EQ-T11 NonRetryable → abandon → DLQ（经 Worker Terminator 分类）。
func TestTShardedEQNonRetryable(t *testing.T) {
	sq := newSharded(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sq.Enqueue(ctx, "eqt11", []byte("bad-input")); err != nil {
		t.Fatal(err)
	}
	w := sq.NewWorker(kiki.WorkerOptions{
		Concurrency:       1,
		PollInterval:      5 * time.Millisecond,
		ShutdownGrace:     2 * time.Second,
		HeartbeatInterval: kiki.HeartbeatDisabled,
	})
	w.Handle(func(ctx context.Context, task kiki.Task) error {
		return kiki.NonRetryable(errors.New("bad input"))
	})
	done := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(done)
	}()
	waitFor(t, 5*time.Second, "abandon dead letter", func() bool {
		entries, err := sq.ListDLQ(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.ID == "eqt11" && e.Via == "abandon" {
				return true
			}
		}
		return false
	})
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not shut down")
	}
}
