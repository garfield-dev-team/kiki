package kiki

// ShardedQueue 单测（纯 Go 逻辑：路由契约、manifest 协议、句柄路由、并发
// 调度器组装）。凡涉及脚本语义的路径全部留给真实 Redis 上的集成用例
// （integration/sharded_test.go T14–T17）——miniredis 的 Lua 子集不作数。

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// ---- 路由契约（docs/sharded-queue.md §5.1，冻结）----

func TestShardOfFrozen(t *testing.T) {
	// FNV-1a 64 的参考值是硬编码锚点：换哈希、改实现、换取模方式都会在此
	// 失败——这正是目的（换路由 = 全量任务重路由）。
	pins := map[string]uint64{
		"abc":       16654208175385433931,
		"t14-job":   12995184737734111320,
		"tenant-42": 2973703394120846818,
		"route-me":  2786674956145842859,
	}
	for key, sum := range pins {
		h := fnv.New64a()
		if n, err := h.Write([]byte(key)); err != nil || n != len(key) {
			t.Fatalf("hash write %q: %d %v", key, n, err)
		}
		if got := h.Sum64(); got != sum {
			t.Fatalf("hash drifted for %q: got %d, frozen %d", key, got, sum)
		}
		for _, n := range []int{2, 8, 64} {
			if got := ShardOf(key, n); got < 0 || got >= n {
				t.Fatalf("ShardOf(%q,%d) = %d out of range", key, n, got)
			}
			if got := ShardOf(key, n); got != int(sum%uint64(n)) {
				t.Fatalf("ShardOf(%q,%d) = %d, want %d", key, n, got, int(sum%uint64(n)))
			}
		}
	}
}

func TestShardOfDeterministicAndEven(t *testing.T) {
	const n = 8
	counts := make([]int, n)
	for i := 0; i < 10_000; i++ {
		k := ShardOf(fmt.Sprintf("dist-%05d", i), n)
		if k != ShardOf(fmt.Sprintf("dist-%05d", i), n) {
			t.Fatal("routing must be deterministic")
		}
		counts[k]++
	}
	// FNV-1a 对顺序 id 散布良好；±30% 的宽界只为排除实现级劣化而非统计抖动。
	for shard, c := range counts {
		if c < 1250*7/10 || c > 1250*13/10 {
			t.Fatalf("shard %d took %d/10000 tasks — distribution degenerate", shard, c)
		}
	}
}

func TestRouteKeyPreference(t *testing.T) {
	if got := routeKeyOf(Task{ID: "a", routeKey: "tenant-9"}); got != "tenant-9" {
		t.Fatalf("WithRouteKey must win over id, got %q", got)
	}
	if got := routeKeyOf(Task{ID: "a"}); got != "a" {
		t.Fatalf("default route key must be id, got %q", got)
	}
}

// ---- 构造校验（不触网：校验顺序必须先于 manifest 握手）----

func TestNewShardedQueueValidation(t *testing.T) {
	cases := []struct {
		name string
		opts ShardedOptions
		want error
	}{
		{"nil client", ShardedOptions{Name: "q", Shards: 4}, ErrInvalidArgument},
		{"bad name", ShardedOptions{Redis: &redis.Client{}, Name: "bad name!", Shards: 4}, ErrInvalidName},
		{"reserved #", ShardedOptions{Redis: &redis.Client{}, Name: "orders#0", Shards: 4}, ErrInvalidName},
		{"too few shards", ShardedOptions{Redis: &redis.Client{}, Name: "q", Shards: 1}, ErrInvalidArgument},
		{"too many shards", ShardedOptions{Redis: &redis.Client{}, Name: "q", Shards: 65}, ErrInvalidArgument},
	}
	for _, tc := range cases {
		if _, err := NewShardedQueue(tc.opts); !errors.Is(err, tc.want) {
			t.Fatalf("%s: got %v, want %v", tc.name, err, tc.want)
		}
	}
}

// ---- manifest 协议 ----

// fakeRedis 覆写 manifest 相关的四个命令，其余方法经嵌入的 nil *redis.Client
// 满足 UniversalClient 接口（永不被调用）。
type fakeRedis struct {
	*redis.Client
	mu         sync.Mutex
	strings    map[string]string
	depths     map[string]int64
	setnxFails bool // 模拟并发首建竞态的输家
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{strings: map[string]string{}, depths: map[string]int64{}}
}

func (f *fakeRedis) Get(ctx context.Context, key string) *redis.StringCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd := redis.NewStringCmd(ctx)
	if v, ok := f.strings[key]; ok {
		cmd.SetVal(v)
	} else {
		cmd.SetErr(redis.Nil)
	}
	return cmd
}

func (f *fakeRedis) SetNX(ctx context.Context, key string, val interface{}, ttl time.Duration) *redis.BoolCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd := redis.NewBoolCmd(ctx)
	if f.setnxFails {
		cmd.SetVal(false)
		return cmd
	}
	if _, ok := f.strings[key]; ok {
		cmd.SetVal(false)
		return cmd
	}
	f.strings[key] = val.(string)
	cmd.SetVal(true)
	return cmd
}

func (f *fakeRedis) Set(ctx context.Context, key string, val interface{}, ttl time.Duration) *redis.StatusCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.strings[key] = val.(string)
	cmd := redis.NewStatusCmd(ctx)
	cmd.SetVal("OK")
	return cmd
}

func (f *fakeRedis) ZCard(ctx context.Context, key string) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(f.depths[key])
	return cmd
}

func (f *fakeRedis) XLen(ctx context.Context, key string) *redis.IntCmd {
	return f.ZCard(ctx, key)
}

func manifestHandshakeOK(t *testing.T, f *fakeRedis, name string, n int, mode ManifestMode) error {
	t.Helper()
	return manifestHandshake(context.Background(), f, name, n, mode, discardLogger())
}

func TestManifestHandshake(t *testing.T) {
	ctx := context.Background()

	// 首建：不存在 → 以本地 N 创建。
	f := newFakeRedis()
	if err := manifestHandshakeOK(t, f, "orders", 4, ManifestStrict); err != nil {
		t.Fatalf("first build: %v", err)
	}
	n, ok, err := LookupManifest(ctx, f, "orders")
	if err != nil || !ok || n != 4 {
		t.Fatalf("lookup after first build: n=%d ok=%v err=%v", n, ok, err)
	}

	// 一致 → 通过。
	if err := manifestHandshakeOK(t, f, "orders", 4, ManifestStrict); err != nil {
		t.Fatalf("matching handshake: %v", err)
	}

	// 漂移 → Strict 拒绝且带修复指引；Warn 放行；Off 跳过。
	err = manifestHandshakeOK(t, f, "orders", 8, ManifestStrict)
	if !errors.Is(err, errManifestMismatch) {
		t.Fatalf("strict mismatch: want errManifestMismatch, got %v", err)
	}
	if !strings.Contains(err.Error(), "kikictl sq manifest set") {
		t.Fatalf("strict mismatch must carry remediation: %v", err)
	}
	if err := manifestHandshakeOK(t, f, "orders", 8, ManifestWarn); err != nil {
		t.Fatalf("warn mismatch must pass: %v", err)
	}
	if err := manifestHandshakeOK(t, f, "orders", 8, ManifestOff); err != nil {
		t.Fatalf("off must pass: %v", err)
	}

	// 并发首建输家：SetNX 失败后读到对方的 manifest，按其值比较。
	f2 := newFakeRedis()
	f2.setnxFails = true
	f2.strings[manifestKey("hot")] = mustManifest(t, 6)
	if err := manifestHandshakeOK(t, f2, "hot", 6, ManifestStrict); err != nil {
		t.Fatalf("lost race with matching N should pass: %v", err)
	}
	err = manifestHandshakeOK(t, f2, "hot", 8, ManifestStrict)
	if !errors.Is(err, errManifestMismatch) {
		t.Fatalf("lost race with mismatched N should fail strict: %v", err)
	}

	// 损坏的 manifest。
	f3 := newFakeRedis()
	f3.strings[manifestKey("broken")] = "{not json"
	if _, _, err := LookupManifest(ctx, f3, "broken"); err == nil {
		t.Fatal("corrupt manifest must error")
	}
	if err := manifestHandshakeOK(t, f3, "broken", 4, ManifestOff); err != nil {
		t.Fatalf("off must skip even corrupt manifest: %v", err)
	}
}

func mustManifest(t *testing.T, n int) string {
	t.Helper()
	v, err := encodeManifest(n)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestSetManifestGuards(t *testing.T) {
	ctx := context.Background()
	f := newFakeRedis()

	// 初建。
	if err := SetManifest(ctx, f, "orders", 4, false); err != nil {
		t.Fatalf("initial set: %v", err)
	}
	// 扩容自由。
	if err := SetManifest(ctx, f, "orders", 8, false); err != nil {
		t.Fatalf("grow: %v", err)
	}
	n, _, _ := LookupManifest(ctx, f, "orders")
	if n != 8 {
		t.Fatalf("grow did not persist: n=%d", n)
	}

	// 缩容守卫：被移除分片非空 ⇒ 拒绝。
	f.depths["{qk:orders#6}:ready"] = 5
	err := SetManifest(ctx, f, "orders", 4, false)
	if err == nil || !strings.Contains(err.Error(), "orders#6 not empty") {
		t.Fatalf("shrink with non-empty shard must be refused, got %v", err)
	}
	// force 放行（孤儿任务由调用方显式承担）。
	if err := SetManifest(ctx, f, "orders", 4, true); err != nil {
		t.Fatalf("shrink with force: %v", err)
	}
	// 全空 ⇒ 无需 force。
	f.depths["{qk:orders#6}:ready"] = 0
	if err := SetManifest(ctx, f, "orders", 4, false); err != nil {
		t.Fatalf("shrink of drained shards: %v", err)
	}

	// 参数校验。
	if err := SetManifest(ctx, f, "orders#0", 4, false); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("reserved # must be rejected: %v", err)
	}
	if err := SetManifest(ctx, f, "orders", 1, false); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("bad N must be rejected: %v", err)
	}
}

// ---- 分片句柄路由（ErrNoShard）----

func TestShardForHandle(t *testing.T) {
	sq := &ShardedQueue{
		name: "t",
		n:    3,
		log:  discardLogger(),
		metr: nopMetrics,
		shards: []*Queue{
			{log: discardLogger(), metr: nopMetrics, name: "t#0"},
			{log: discardLogger(), metr: nopMetrics, name: "t#1"},
			{log: discardLogger(), metr: nopMetrics, name: "t#2"},
		},
	}
	q, err := sq.shardFor(1, "x")
	if err != nil || q != sq.shards[1] {
		t.Fatalf("valid handle: q=%v err=%v", q, err)
	}
	for _, shard := range []int{shardNone, 3, 99} {
		_, err := sq.shardFor(shard, "x")
		if !errors.Is(err, ErrNoShard) {
			t.Fatalf("shard %d: want ErrNoShard, got %v", shard, err)
		}
	}
	// 门面终结写同样拒绝越界句柄（ErrClosed 优先于 ErrNoShard，不触网）。
	if _, err := sq.Fail(context.Background(), Task{ID: "a", Shard: 7}, errors.New("boom"), 0); !errors.Is(err, ErrNoShard) {
		t.Fatalf("facade Fail: want ErrNoShard, got %v", err)
	}
	if err := sq.Heartbeat(context.Background(), Task{ID: "a", Shard: shardNone}, time.Second); !errors.Is(err, ErrNoShard) {
		t.Fatalf("facade Heartbeat: want ErrNoShard, got %v", err)
	}
}

func TestShardedClosed(t *testing.T) {
	sq := &ShardedQueue{name: "t", n: 2, log: discardLogger(), metr: nopMetrics,
		shards: []*Queue{{log: discardLogger(), metr: nopMetrics, name: "t#0"}, {log: discardLogger(), metr: nopMetrics, name: "t#1"}}}
	if err := sq.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := sq.checkOpen(); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed after Close, got %v", err)
	}
	for _, q := range sq.shards {
		if err := q.checkOpen(); !errors.Is(err, ErrClosed) {
			t.Fatalf("Close must cascade to shard %s, got %v", q.name, err)
		}
	}
}

// ---- 并发调度器组装（-race 覆盖 goroutine 退出路径）----

func TestMultiSchedulerJoinsAndExits(t *testing.T) {
	mk := func() *Scheduler {
		return &Scheduler{
			q:    &Queue{log: discardLogger(), metr: nopMetrics, name: "t"},
			opts: SchedulerOptions{Interval: time.Hour}, // 不触发 tick：tick 会触网
			log:  discardLogger(),
			rng:  rngForTest(),
		}
	}
	m := &multiScheduler{scheds: []*Scheduler{mk(), mk(), mk()}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		m.Run(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("multiScheduler.Run must exit once all shard schedulers return")
	}
}
