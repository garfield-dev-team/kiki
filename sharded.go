package kiki

// ShardedQueue（v0.2）：一个逻辑队列 = N 个物理队列 `name#0..N-1` 的纯 SDK
// 合成视图（规范源头 docs/sharded-queue.md）。本文件只做三件事：路由（纯
// 函数）、manifest 契约（N 的事实源）、门面扇出/转发——一切状态转移仍然
// 只发生在 scripts/*.lua 里，每分片独立跑 v1 全套机制（fencing / tries /
// ver / 双毒丸防线 / 无主 sweep），零脚本改动。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/garfield-dev-team/kiki/internal/metrics"
	"github.com/garfield-dev-team/kiki/internal/rdb"
)

// ShardsMin/ShardsMax 是 N 的合法区间。上界依据 docs/sharded-queue.md §10.2：
// 约束不是哈希质量而是空转轮询税与指标基数；更大的规模应先拆业务队列。
const (
	ShardsMin = 2
	ShardsMax = 64
)

// ShardOf 是分片路由函数：fnv1a64(key) mod n。★ 冻结契约——一经发布不得
// 更换（更换 = 全量任务重路由，破坏 WithRouteKey 保序与迁移假设），
// docs/sharded-queue.md §5.1。
func ShardOf(key string, n int) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum64() % uint64(n))
}

// routeKeyOf 返回任务的分片路由键：WithRouteKey 显式指定者优先，否则按 id。
func routeKeyOf(t Task) string {
	if t.routeKey != "" {
		return t.routeKey
	}
	return t.ID
}

// ManifestMode 控制 NewShardedQueue 对 manifest（N 的事实源，§9.2）的校验
// 力度。零值即默认。
type ManifestMode int

const (
	// ManifestStrict（默认）：本地 N 与 manifest 不一致 ⇒ 拒绝启动（fail-fast，
	// 把配置漂移挡在进程外）。
	ManifestStrict ManifestMode = iota
	// ManifestWarn：不一致只记 error 日志，放行启动。迁移窗口期/可用性优先
	// 场景显式开启——这个决定要交给运维，而不是默默容忍。
	ManifestWarn
	// ManifestOff：跳过校验（测试/特殊环境）。生产禁用。
	ManifestOff
)

// ShardedOptions 是 ShardedQueue 配置。QueueOptions 内的 Redis/Name 被本
// 结构覆盖，其余字段逐个复用于每个分片（同一套配额 ⇒ 分片间语义一致，
// docs/sharded-queue.md §7.4）。
type ShardedOptions struct {
	Redis redis.UniversalClient
	// Name 是逻辑队列名。禁止含 '#'（那是物理队列命名空间，task.go queueRe 预留）。
	Name string
	// Shards 是分片数 N ∈ [ShardsMin, ShardsMax]，schema 级静态契约（§9）。
	Shards int
	// QueueOptions 复用于每个分片；其 Redis/Name 字段被忽略。
	QueueOptions QueueOptions
	// ManifestCheck 默认 ManifestStrict。
	ManifestCheck ManifestMode

	Logger *slog.Logger
}

// ShardedStats 是合并视图的深度快照：聚合值 + 分片明细（"卡分片"检测与
// 容量诊断的原始数据，docs/sharded-queue.md §8.3）。
type ShardedStats struct {
	Stats  // Depth = Σ分片；OldestReadyAge = max(分片)
	Shards []Stats
}

// ShardedQueue 是逻辑队列门面。构造后不可变（与 Queue 同款 atomic closed
// flag），可并发使用。
type ShardedQueue struct {
	name   string // 逻辑名
	shards []*Queue
	n      int
	visCap time.Duration
	log    *slog.Logger
	metr   metrics.Interface // 逻辑名打点：Worker reserve 计数与 handler 直方图

	closed   atomic.Bool
	schedUse atomic.Bool
}

// Worker 不感知单队列/分片视图的编译期证明。
var _ engine = (*ShardedQueue)(nil)

// NewShardedQueue 构造分片合并视图：manifest 握手 → 逐分片 NewQueue（各自
// warmup 脚本）。manifest 不存在时以本地 N 创建（SETNX，并发首建竞态下后到
// 者按 Strict 比较后失败退出——暴露配置分歧正是想要的）。
func NewShardedQueue(opts ShardedOptions) (*ShardedQueue, error) {
	if opts.Redis == nil {
		return nil, fmt.Errorf("kiki: %w: Redis client is nil", ErrInvalidArgument)
	}
	if err := validateQueueName(opts.Name); err != nil {
		return nil, err
	}
	if strings.ContainsRune(opts.Name, '#') {
		return nil, fmt.Errorf("%w: %q contains reserved '#' (physical shard namespace)", ErrInvalidName, opts.Name)
	}
	if opts.Shards < ShardsMin || opts.Shards > ShardsMax {
		return nil, fmt.Errorf("%w: Shards=%d out of [%d,%d]", ErrInvalidArgument, opts.Shards, ShardsMin, ShardsMax)
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	log = log.With("queue", opts.Name, "shards", opts.Shards)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := manifestHandshake(ctx, opts.Redis, opts.Name, opts.Shards, opts.ManifestCheck, log); err != nil {
		return nil, err
	}

	shards := make([]*Queue, 0, opts.Shards)
	for k := 0; k < opts.Shards; k++ {
		sub := opts.QueueOptions
		sub.Redis = opts.Redis
		sub.Name = shardName(opts.Name, k)
		q, err := NewQueue(sub)
		if err != nil {
			return nil, fmt.Errorf("shard %d: %w", k, err)
		}
		shards = append(shards, q)
	}
	return &ShardedQueue{
		name:   opts.Name,
		shards: shards,
		n:      opts.Shards,
		visCap: shards[0].engineVisCap(),
		log:    log,
		// 分片各自的 Queue 已按物理名打点；逻辑名打点供 Worker/中间件与
		// "按逻辑队列聚合"的仪表盘使用（docs/sharded-queue.md §8.3）。
		metr: metrics.New(opts.QueueOptions.MetricsRegisterer, opts.Name),
	}, nil
}

func shardName(logical string, k int) string {
	return fmt.Sprintf("%s#%d", logical, k)
}

// Name 返回逻辑队列名。
func (sq *ShardedQueue) Name() string { return sq.name }

// Shards 返回分片数 N。
func (sq *ShardedQueue) Shards() int { return sq.n }

// Close 关闭全部分片（不关闭用户传入的 Redis client，所有权在调用方）。
func (sq *ShardedQueue) Close() error {
	sq.closed.Store(true)
	for _, q := range sq.shards {
		_ = q.Close()
	}
	return nil
}

func (sq *ShardedQueue) checkOpen() error {
	if sq.closed.Load() {
		return ErrClosed
	}
	return nil
}

// shardFor 校验终结写携带的分片句柄并返回目标分片。越界 ⇒ ErrNoShard：
// 处置同级 ErrFenced——不重试、不吞（docs/sharded-queue.md §5.3）。
func (sq *ShardedQueue) shardFor(shard int, id string) (*Queue, error) {
	if shard < 0 || shard >= sq.n {
		return nil, fmt.Errorf("%w: shard %d out of [0,%d) for task %s", ErrNoShard, shard, sq.n, id)
	}
	return sq.shards[shard], nil
}

// ---- 生产侧（§6）----

// Enqueue 按路由键（WithRouteKey 或 id）投递到唯一分片，语义与 v1 一致。
func (sq *ShardedQueue) Enqueue(ctx context.Context, id string, payload []byte, opts ...EnqueueOption) error {
	if err := sq.checkOpen(); err != nil {
		return err
	}
	t := Task{ID: id, Payload: payload}
	for _, o := range opts {
		o(&t)
	}
	// 统一校验面：各分片 QueueOptions 同构，取 0 号分片的校验规则即可。
	if err := sq.shards[0].validateTask(&t); err != nil {
		return err
	}
	k := ShardOf(routeKeyOf(t), sq.n)
	return sq.shards[k].Enqueue(ctx, id, payload, opts...)
}

// EnqueueIn 延迟投递，路由同 Enqueue。
func (sq *ShardedQueue) EnqueueIn(ctx context.Context, id string, payload []byte, delay time.Duration, opts ...EnqueueOption) error {
	if err := sq.checkOpen(); err != nil {
		return err
	}
	t := Task{ID: id, Payload: payload}
	for _, o := range opts {
		o(&t)
	}
	t.Delay = delay
	if err := sq.shards[0].validateTask(&t); err != nil {
		return err
	}
	return sq.shards[ShardOf(routeKeyOf(t), sq.n)].EnqueueIn(ctx, id, payload, delay, opts...)
}

// EnqueueBulk 按 task 逐个路由，同分片的任务合并进该分片的单次 pipeline。
// 校验失败整批拒绝（与 v1 一致，不产生半批）；脚本级失败逐条报告并 join。
// 跨分片无 all-or-nothing——那在 Redis Cluster 上架构级不存在（§2 非目标）。
func (sq *ShardedQueue) EnqueueBulk(ctx context.Context, tasks []Task) error {
	if err := sq.checkOpen(); err != nil {
		return err
	}
	byShard := make([][]Task, sq.n)
	for i := range tasks {
		if err := sq.shards[0].validateTask(&tasks[i]); err != nil {
			return fmt.Errorf("kiki: task %d (%s): %w", i, tasks[i].ID, err)
		}
		k := ShardOf(routeKeyOf(tasks[i]), sq.n)
		byShard[k] = append(byShard[k], tasks[i])
	}
	var errs []error
	for k, group := range byShard {
		if len(group) == 0 {
			continue
		}
		if err := sq.shards[k].EnqueueBulk(ctx, group); err != nil {
			errs = append(errs, fmt.Errorf("shard %d: %w", k, err))
		}
	}
	return errors.Join(errs...)
}

// ---- 消费侧（§7）----

// Reserve 以进程级默认 owner 领取（独立消费者/测试用）。
func (sq *ShardedQueue) Reserve(ctx context.Context, vis time.Duration, max int) ([]Task, error) {
	return sq.ReserveFor(ctx, defaultOwner(), vis, max)
}

// ReserveFor 乱序轮询各分片直至领满 max：每次调用重新洗牌 ⇒ 多消费者对分片
// 的抢占长期均匀；积压时命中即停。返回的 Task 携带 Shard 句柄，终结写由此
// 路由回原分片——任务全生命周期锁死单一 hash tag（§6.3）。vis 钳制由分片
// 自行执行（各分片同上限）。
func (sq *ShardedQueue) ReserveFor(ctx context.Context, worker string, vis time.Duration, max int) ([]Task, error) {
	if err := sq.checkOpen(); err != nil {
		return nil, err
	}
	if max < 1 {
		max = 1
	}
	var out []Task
	for _, k := range rand.Perm(sq.n) { // math/rand 全局源并发安全；顺序仅影响调度不影响正确性
		ts, err := sq.shards[k].ReserveFor(ctx, worker, vis, max-len(out))
		if err != nil {
			return nil, err
		}
		for i := range ts {
			ts[i].Shard = k
		}
		out = append(out, ts...)
		if len(out) >= max {
			break
		}
	}
	return out, nil
}

// Complete 幂等终结：按 Task.Shard 路由回原分片。
func (sq *ShardedQueue) Complete(ctx context.Context, t Task, result string) error {
	if err := sq.checkOpen(); err != nil {
		return err
	}
	q, err := sq.shardFor(t.Shard, t.ID)
	if err != nil {
		return err
	}
	return q.Complete(ctx, t, result)
}

// Fail 报告失败：按 Task.Shard 路由回原分片。
func (sq *ShardedQueue) Fail(ctx context.Context, t Task, cause error, backoff time.Duration) (FailResult, error) {
	if err := sq.checkOpen(); err != nil {
		return Retried, err
	}
	q, err := sq.shardFor(t.Shard, t.ID)
	if err != nil {
		return Retried, err
	}
	return q.Fail(ctx, t, cause, backoff)
}

// abandon 直接死信非 retryable 失败（Worker 内部路径）。
func (sq *ShardedQueue) abandon(ctx context.Context, t Task, cause error) (FailResult, error) {
	if err := sq.checkOpen(); err != nil {
		return Retried, err
	}
	q, err := sq.shardFor(t.Shard, t.ID)
	if err != nil {
		return Retried, err
	}
	return q.abandon(ctx, t, cause)
}

// Heartbeat 续约：按 Task.Shard 路由回原分片。
func (sq *ShardedQueue) Heartbeat(ctx context.Context, t Task, extend time.Duration) error {
	if err := sq.checkOpen(); err != nil {
		return err
	}
	q, err := sq.shardFor(t.Shard, t.ID)
	if err != nil {
		return err
	}
	return q.Heartbeat(ctx, t, extend)
}

// Release 归还任务：按 Task.Shard 路由回原分片。
func (sq *ShardedQueue) Release(ctx context.Context, t Task) error {
	if err := sq.checkOpen(); err != nil {
		return err
	}
	q, err := sq.shardFor(t.Shard, t.ID)
	if err != nil {
		return err
	}
	return q.Release(ctx, t)
}

// ---- 运维面（§8）----

// Stats 扇出聚合：Depth 求和、OldestReadyAge 取 max，分片明细附在 Shards。
func (sq *ShardedQueue) Stats(ctx context.Context) (ShardedStats, error) {
	var out ShardedStats
	if err := sq.checkOpen(); err != nil {
		return out, err
	}
	out.Shards = make([]Stats, 0, sq.n)
	for _, q := range sq.shards {
		st, err := q.Stats(ctx)
		if err != nil {
			return out, err
		}
		out.ReadyDepth += st.ReadyDepth
		out.SchedDepth += st.SchedDepth
		out.LeaseDepth += st.LeaseDepth
		out.DLQLen += st.DLQLen
		if st.OldestReadyAge > out.OldestReadyAge {
			out.OldestReadyAge = st.OldestReadyAge
		}
		out.Shards = append(out.Shards, st)
	}
	return out, nil
}

// ListDLQ 扇出各分片（每分片至多 count 条）后按 TS 升序合并取前 count 条
// ——与 v1"从最旧开始浏览"的语义一致。条目携带 Shard，回放经它路由。
func (sq *ShardedQueue) ListDLQ(ctx context.Context, count int64) ([]DLQEntry, error) {
	if err := sq.checkOpen(); err != nil {
		return nil, err
	}
	var all []DLQEntry
	for k, q := range sq.shards {
		entries, err := q.ListDLQ(ctx, count)
		if err != nil {
			return nil, err
		}
		for i := range entries {
			entries[i].Shard = k
		}
		all = append(all, entries...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].TS.Before(all[j].TS) })
	if int64(len(all)) > count {
		all = all[:count]
	}
	return all, nil
}

// ReplayDLQ 按条目的 Shard 分组后回各自分片执行 replay.lua（v1 语义原样；
// state==DLQ 守卫保证重复执行安全，故无跨分片原子性需求）。
func (sq *ShardedQueue) ReplayDLQ(ctx context.Context, entries []DLQEntry, force bool) error {
	if err := sq.checkOpen(); err != nil {
		return err
	}
	byShard := make(map[int][]DLQEntry, sq.n)
	for _, e := range entries {
		if _, err := sq.shardFor(e.Shard, e.ID); err != nil {
			return err
		}
		byShard[e.Shard] = append(byShard[e.Shard], e)
	}
	for k, group := range byShard {
		if err := sq.shards[k].ReplayDLQ(ctx, group, force); err != nil {
			return fmt.Errorf("shard %d: %w", k, err)
		}
	}
	return nil
}

// SweepOnce 扇出全分片各执行一轮 sweep+promote，结果求和。
func (sq *ShardedQueue) SweepOnce(ctx context.Context, limit int) (SweepStats, error) {
	var out SweepStats
	if err := sq.checkOpen(); err != nil {
		return out, err
	}
	for k, q := range sq.shards {
		st, err := q.SweepOnce(ctx, limit)
		if err != nil {
			return out, fmt.Errorf("shard %d: %w", k, err)
		}
		out.Requeued += st.Requeued
		out.DeadLettered += st.DeadLettered
	}
	return out, nil
}

// ---- Worker / Scheduler 接入（§7.3、§8.1）----

// NewWorker 构造跑在合并视图上的 Worker：reserve 走乱序轮询，终结写按
// Task.Shard 路由。首个 Worker 一次拉起全部 N 个分片级 scheduler。
func (sq *ShardedQueue) NewWorker(opts WorkerOptions) *Worker {
	return newWorkerFor(sq, opts)
}

func (sq *ShardedQueue) engineName() string               { return sq.name }
func (sq *ShardedQueue) engineVisCap() time.Duration      { return sq.visCap }
func (sq *ShardedQueue) engineMetrics() metrics.Interface { return sq.metr }
func (sq *ShardedQueue) engineLogger() *slog.Logger       { return sq.log }

func (sq *ShardedQueue) acquireScheduler() bool { return !sq.schedUse.Swap(true) }

// newScheduler 返回覆盖全部分片的调度器：每分片一个（无主幂等，重叠运行
// 无害），OnDLQ hook 补上 Shard 标注后透传。
func (sq *ShardedQueue) newScheduler(opts SchedulerOptions) schedulerRunner {
	scheds := make([]*Scheduler, 0, sq.n)
	for k, q := range sq.shards {
		s := opts
		if s.OnDLQ != nil {
			onDLQ := s.OnDLQ
			s.OnDLQ = func(t Task, via string, cause error) {
				t.Shard = k
				onDLQ(t, via, cause)
			}
		}
		scheds = append(scheds, q.NewScheduler(s))
	}
	return &multiScheduler{scheds: scheds}
}

// multiScheduler 并行运行 N 个分片级 scheduler，全部退出才返回。
type multiScheduler struct {
	scheds []*Scheduler
}

func (m *multiScheduler) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, s := range m.scheds {
		wg.Add(1)
		go func(s *Scheduler) {
			defer wg.Done()
			s.Run(ctx)
		}(s)
	}
	wg.Wait()
}

// ---- manifest：N 的事实源（§9.2）----
//
// manifest 不是任务状态机的一部分——它与 dedup 键同属"刻意无 hash tag 的
// 运维元数据"（go-implementation.md §3.3 先例），经普通 SET/GET 写读，
// 不违反"状态转移唯一写入者"红线。

const manifestPrefix = "kiki:sqmanifest:"

func manifestKey(logical string) string { return manifestPrefix + logical }

type manifestDoc struct {
	N    int    `json:"n"`
	TsMs int64  `json:"ts_ms"` // 信息性字段（runbook 排查用），不参与任何决策
	By   string `json:"by"`
}

func encodeManifest(n int) (string, error) {
	host, err := os.Hostname()
	if err != nil {
		host = "?"
	}
	b, err := json.Marshal(manifestDoc{N: n, TsMs: time.Now().UnixMilli(), By: host + ":" + strconv.Itoa(os.Getpid())})
	if err != nil {
		return "", fmt.Errorf("kiki: encode manifest: %w", err)
	}
	return string(b), nil
}

func decodeManifest(raw string) (manifestDoc, error) {
	var m manifestDoc
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return m, fmt.Errorf("kiki: decode manifest: %w", err)
	}
	if m.N < ShardsMin || m.N > ShardsMax {
		return m, fmt.Errorf("kiki: manifest n=%d out of [%d,%d]", m.N, ShardsMin, ShardsMax)
	}
	return m, nil
}

// errManifestMismatch 是本地 N 与 manifest 不一致的判别锚点（errors.Is）。
var errManifestMismatch = errors.New("kiki: sharded queue manifest mismatch")

// manifestHandshake 是 NewShardedQueue 的启动期校验（运行期不复查——进程
// 持续可用性不依赖 manifest）。
func manifestHandshake(ctx context.Context, c redis.UniversalClient, name string, n int, mode ManifestMode, log *slog.Logger) error {
	if mode == ManifestOff {
		return nil
	}
	key := manifestKey(name)
	raw, err := c.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		v, encErr := encodeManifest(n)
		if encErr != nil {
			return encErr
		}
		created, setErr := c.SetNX(ctx, key, v, 0).Result()
		if setErr != nil {
			return fmt.Errorf("kiki: create manifest %s: %w", name, setErr)
		}
		if created {
			return nil
		}
		// 并发首建：对方赢了对我们就是"读到既有 manifest"，走下面的比较。
		raw, err = c.Get(ctx, key).Result()
	}
	if err != nil {
		return fmt.Errorf("kiki: read manifest %s: %w", name, err)
	}
	got, err := decodeManifest(raw)
	if err != nil {
		return fmt.Errorf("kiki: manifest %s: %w", name, err)
	}
	if got.N == n {
		return nil
	}
	if mode == ManifestWarn {
		log.Error("sharded queue manifest mismatch; continuing (ManifestWarn)",
			"manifest_n", got.N, "local_n", n,
			"remediation", "kikictl sq manifest set "+name+" --shards "+strconv.Itoa(n))
		return nil
	}
	return fmt.Errorf("%w for %q: manifest n=%d, local n=%d (remediation: kikictl sq manifest set %s --shards %d, or fix local Shards)",
		errManifestMismatch, name, got.N, n, name, got.N)
}

// LookupManifest 返回逻辑队列的 manifest N（不存在时 ok=false）。运维工具
// 据此探测"这是分片队列还是单队列"。
func LookupManifest(ctx context.Context, c redis.UniversalClient, name string) (n int, ok bool, err error) {
	raw, err := c.Get(ctx, manifestKey(name)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	m, err := decodeManifest(raw)
	if err != nil {
		return 0, false, err
	}
	return m.N, true, nil
}

// SetManifest 写入/变更 manifest——N 迁移的唯一入口（kikictl sq manifest）。
// 只允许单调扩 N；缩 N 必须被移除分片四项深度全为 0（尽力而为检查），否则
// 显式 force 并接受孤儿任务（docs/sharded-queue.md §9.4）。
func SetManifest(ctx context.Context, c redis.UniversalClient, name string, n int, force bool) error {
	if err := validateQueueName(name); err != nil {
		return err
	}
	if strings.ContainsRune(name, '#') {
		return fmt.Errorf("%w: %q contains reserved '#'", ErrInvalidName, name)
	}
	if n < ShardsMin || n > ShardsMax {
		return fmt.Errorf("%w: Shards=%d out of [%d,%d]", ErrInvalidArgument, n, ShardsMin, ShardsMax)
	}
	cur, ok, err := LookupManifest(ctx, c, name)
	if err != nil {
		return err
	}
	if !ok {
		v, err := encodeManifest(n)
		if err != nil {
			return err
		}
		return c.SetNX(ctx, manifestKey(name), v, 0).Err()
	}
	if n == cur {
		return nil
	}
	if n < cur && !force {
		for k := n; k < cur; k++ {
			empty, err := shardEmpty(ctx, c, name, k)
			if err != nil {
				return err
			}
			if !empty {
				return fmt.Errorf("kiki: shard %s#%d not empty; drain it first or pass force (orphans become reachable only as a bare queue)", name, k)
			}
		}
	}
	v, err := encodeManifest(n)
	if err != nil {
		return err
	}
	return c.Set(ctx, manifestKey(name), v, 0).Err()
}

// shardEmpty 尽力而为检查被移除分片的四项深度（检查后写入的窗口存在，
// 所以这只是护栏不是事务）。
func shardEmpty(ctx context.Context, c redis.UniversalClient, logical string, k int) (bool, error) {
	keys := rdb.BuildKeys(shardName(logical, k))
	for _, key := range []string{keys.Ready, keys.Sched, keys.Lease} {
		depth, err := c.ZCard(ctx, key).Result()
		if err != nil {
			return false, err
		}
		if depth > 0 {
			return false, nil
		}
	}
	dlq, err := c.XLen(ctx, keys.DLQ).Result()
	if err != nil {
		return false, err
	}
	return dlq == 0, nil
}
