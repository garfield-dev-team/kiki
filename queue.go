package kiki

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/garfield-dev-team/kiki/internal/metrics"
	"github.com/garfield-dev-team/kiki/internal/rdb"
)

// Version 汇报库版本（脚本随库编译期冻结，见 go-implementation.md §12）。
const Version = "v0.1.0"

// schemaVersion 写入每个 task hash 的 sv 字段，供未来迁移脚本分批 HSET。
const schemaVersion = 1

const defaultReadTimeout = time.Second // 脚本都是毫秒级，1s 已含重试余量（§3.1）

// QueueOptions 是队列级配置。零值字段取默认值。
type QueueOptions struct {
	Redis redis.UniversalClient
	Name  string

	// MaxRetries 是队列默认重试上限（首投之外的额外重试次数）；
	// per-task WithMaxRetries 覆盖。上限不做代码强制，是有意的运营契约。
	MaxRetries int // 默认 5
	// MaxRedeliveries 是 sweep 路径 lease_resets 的毒丸上限（转移 #10）。
	MaxRedeliveries int // 默认 20
	// Retention 是终态 task hash 的保留期（EXPIRE）。
	Retention time.Duration // 默认 24h
	// PayloadLimit 之外的纪律：>100KB 建议对象存储指针（design.md §9）。
	PayloadLimit int64 // 默认 1 MiB
	// DLQMaxLen 是 DLQ Stream 的 XTRIM MAXLEN ~ 封顶。
	DLQMaxLen int64 // 默认 10000
	// VisibilityTimeout 是 vis 的队列上限；worker 可下调不可上调。
	// 禁止用调大 vis 代替心跳。
	VisibilityTimeout time.Duration // 默认 30s

	Logger            *slog.Logger
	MetricsRegisterer prometheus.Registerer // nil → 全部指标 noop
}

// Queue 是引擎门面：持有 key 布局与全部脚本。业务代码只允许经 Worker/生产侧
// API 使用它；任何绕过门面的直写都会破坏状态机完整性。
type Queue struct {
	name string
	eng  *rdb.Engine
	log  *slog.Logger
	metr metrics.Interface

	maxRetries      int
	maxRedeliveries int
	retention       time.Duration
	payloadLimit    int64
	dlqMaxLen       int64
	visCap          time.Duration

	closed       atomic.Bool
	schedulerUse atomic.Bool

	schedMu   sync.Mutex
	scheduler *Scheduler
}

// NewQueue 构造队列并 best-effort warmup 全部脚本。
func NewQueue(opts QueueOptions) (*Queue, error) {
	if opts.Redis == nil {
		return nil, fmt.Errorf("kiki: %w: Redis client is nil", ErrInvalidArgument)
	}
	if err := validateQueueName(opts.Name); err != nil {
		return nil, err
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	eng, err := rdb.New(opts.Redis, opts.Name, scriptFS, defaultReadTimeout)
	if err != nil {
		return nil, err
	}
	q := &Queue{
		name:            opts.Name,
		eng:             eng,
		log:             log.With("queue", opts.Name),
		metr:            metrics.New(opts.MetricsRegisterer, opts.Name),
		maxRetries:      orDefault(opts.MaxRetries, 5, positive),
		maxRedeliveries: orDefault(opts.MaxRedeliveries, 20, positive),
		retention:       orDefault(opts.Retention, 24*time.Hour, positiveDur),
		payloadLimit:    orDefault(opts.PayloadLimit, 1<<20, positive64),
		dlqMaxLen:       orDefault(opts.DLQMaxLen, 10000, positive64),
		visCap:          orDefault(opts.VisibilityTimeout, 30*time.Second, positiveDur),
	}
	// 首次调用的 NOSCRIPT 回退抖动挪到启动期；失败仅告警（Run 自带回退）。
	warmCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := eng.Warmup(warmCtx); err != nil {
		q.log.Warn("script warmup failed; falling back to lazy EVAL", "err", err)
	}
	return q, nil
}

// 零值 = 取默认（§9 调参表）；负值非法。注意 MaxRetries/MaxRedeliveries 的
// "完全关闭重试/重投上限" 无法经零值表达——那是有意的：这两个上限是毒丸
// 防线，默认打开。
func positive(v int) bool              { return v > 0 }
func positive64(v int64) bool          { return v > 0 }
func positiveDur(v time.Duration) bool { return v > 0 }

func orDefault[T int | int64 | time.Duration](v T, dflt T, ok func(T) bool) T {
	if ok(v) {
		return v
	}
	return dflt
}

// Name 返回队列名。
func (q *Queue) Name() string { return q.name }

// Version 返回库版本与 schema 版本（供 kikictl 探测混部版本）。
func (q *Queue) Version() string { return fmt.Sprintf("%s schema=%d", Version, schemaVersion) }

// Close 标记队列关闭。不关闭用户传入的 Redis client（所有权在调用方）。
func (q *Queue) Close() error {
	q.closed.Store(true)
	return nil
}

func (q *Queue) checkOpen() error {
	if q.closed.Load() {
		return ErrClosed
	}
	return nil
}

// Client 暴露底层连接供只读运维命令（ZCARD/XRANGE/HGETALL）使用。
// 一切写路径必须经脚本——这是纪律不是建议。
func (q *Queue) Client() redis.UniversalClient { return q.eng.Client() }

// ---- 状态转移操作（引擎级，Worker 运行时建立在它之上，§2.3）----

// Reserve 领取至多 max 个任务（进程级默认 owner，独立消费者/测试用）。
// Worker 运行时经 ReserveFor 以自身 worker id 领取。vis 不可超过队列上限。
func (q *Queue) Reserve(ctx context.Context, vis time.Duration, max int) ([]Task, error) {
	return q.ReserveFor(ctx, q.defaultOwner(), vis, max)
}

func (q *Queue) defaultOwner() string {
	host, err := os.Hostname()
	if err != nil {
		host = "?"
	}
	return fmt.Sprintf("sdk:%s:%d", host, os.Getpid())
}

// ReserveFor 以显式 worker 身份领取（owner 写入 task hash，供脚本双重校验）。
func (q *Queue) ReserveFor(ctx context.Context, worker string, vis time.Duration, max int) ([]Task, error) {
	if err := q.checkOpen(); err != nil {
		return nil, err
	}
	if max < 1 {
		max = 1
	}
	if vis > q.visCap {
		q.log.Warn("visibility timeout clamped to queue cap; use heartbeat for long tasks, do not raise vis",
			"requested", vis.String(), "cap", q.visCap.String())
		vis = q.visCap
	}
	k := q.eng.Keys()
	reply, err := q.eng.Run(ctx, "reserve", []string{k.Ready, k.Lease},
		worker, vis.Milliseconds(), max, k.TaskPrefix)
	if err != nil {
		return nil, err
	}
	out := make([]Task, 0, len(reply))
	for _, row := range reply {
		cols, err := rdb.AsList(row)
		if err != nil || len(cols) != 6 {
			return nil, fmt.Errorf("kiki: reserve: malformed reply row (len=%d, err=%v)", len(cols), err)
		}
		get := rdb.AsString
		id, err := get(cols[0])
		if err != nil {
			return nil, err
		}
		ver, err := get(cols[1])
		if err != nil {
			return nil, err
		}
		tries, err := get(cols[2])
		if err != nil {
			return nil, err
		}
		dl, err := get(cols[3])
		if err != nil {
			return nil, err
		}
		payload, err := get(cols[4])
		if err != nil {
			return nil, err
		}
		hdr, err := get(cols[5])
		if err != nil {
			return nil, err
		}
		verN, _ := strconv.ParseInt(ver, 10, 64)
		triesN, _ := strconv.ParseInt(tries, 10, 64)
		dlMs, _ := strconv.ParseInt(dl, 10, 64)
		var headers map[string]string
		if hdr != "" {
			if err := json.Unmarshal([]byte(hdr), &headers); err != nil {
				return nil, fmt.Errorf("kiki: decode headers of %s: %w", id, err)
			}
		}
		out = append(out, Task{
			ID:            id,
			Payload:       []byte(payload),
			Headers:       headers,
			Owner:         worker,
			Tries:         int(triesN),
			Ver:           verN,
			LeaseDeadline: time.UnixMilli(dlMs),
		})
	}
	return out, nil
}

// Complete 幂等地完成任务（转移 #6）。响应丢失后的重试得到 nil（OK_DUP，
// 计入 complete_total{result=dup}）。收到 ErrFenced 时调用方必须放弃。
func (q *Queue) Complete(ctx context.Context, t Task, result string) error {
	if err := q.checkOpen(); err != nil {
		return err
	}
	k := q.eng.Keys()
	st, _, err := q.call(ctx, "complete", []string{k.Lease, k.TaskPrefix + t.ID},
		t.Owner, t.Ver, t.ID, result, q.retention.Milliseconds())
	if err != nil {
		return err
	}
	switch st {
	case "OK":
		q.metr.CompleteTotal("ok")
	case "OK_DUP":
		q.metr.CompleteTotal("dup")
	default:
		q.metr.CompleteTotal(q.resultOf(st))
		return statusError(st, nil)
	}
	return nil
}

// Fail 报告失败（转移 #7/#8）：tries ≤ MaxRetries 时按 backoff 重排
// （Retried），否则进 DLQ（DeadLettered）。退避策略在客户端算好传入。
func (q *Queue) Fail(ctx context.Context, t Task, cause error, backoff time.Duration) (FailResult, error) {
	if err := q.checkOpen(); err != nil {
		return Retried, err
	}
	if backoff < 0 {
		backoff = 0
	}
	k := q.eng.Keys()
	st, _, err := q.call(ctx, "fail", []string{k.Lease, k.Sched, k.DLQ, k.TaskPrefix + t.ID},
		t.Owner, t.Ver, t.ID, errText(cause), backoff.Milliseconds(), q.retention.Milliseconds())
	if err != nil {
		return Retried, err
	}
	switch st {
	case "RETRY":
		q.metr.FailTotal("retry")
		return Retried, nil
	case "DLQ":
		q.metr.FailTotal("dlq")
		q.metr.DLQTotal("fail")
		return DeadLettered, nil
	default:
		res := q.resultOf(st)
		q.metr.FailTotal(res)
		if st == "ERR_FENCED" {
			q.metr.FencedTotal("fail")
		}
		return Retried, statusError(st, nil)
	}
}

// abandon 直接死信非 retryable 失败（abandon.lua，via='abandon'）。
func (q *Queue) abandon(ctx context.Context, t Task, cause error) (FailResult, error) {
	if err := q.checkOpen(); err != nil {
		return Retried, err
	}
	k := q.eng.Keys()
	st, _, err := q.call(ctx, "abandon", []string{k.Lease, k.DLQ, k.TaskPrefix + t.ID},
		t.Owner, t.Ver, t.ID, errText(cause), q.retention.Milliseconds())
	if err != nil {
		return Retried, err
	}
	switch st {
	case "DLQ":
		q.metr.FailTotal("dlq")
		q.metr.DLQTotal("abandon")
		return DeadLettered, nil
	default:
		res := q.resultOf(st)
		q.metr.FailTotal(res)
		if st == "ERR_FENCED" {
			q.metr.FencedTotal("fail")
		}
		return Retried, statusError(st, nil)
	}
}

// Heartbeat 续约（转移 #5）。间隔建议 vis/3：丢一次不至于超时，连续两次
// 丢失应告警。
func (q *Queue) Heartbeat(ctx context.Context, t Task, extend time.Duration) error {
	if err := q.checkOpen(); err != nil {
		return err
	}
	k := q.eng.Keys()
	st, _, err := q.call(ctx, "heartbeat", []string{k.Lease, k.TaskPrefix + t.ID},
		t.Owner, t.Ver, t.ID, extend.Milliseconds())
	if err != nil {
		return err
	}
	if st != "OK" {
		if st == "ERR_FENCED" {
			q.metr.FencedTotal("hb")
		}
		return statusError(st, nil)
	}
	return nil
}

// Release 优雅下线时归还任务（转移 #11）：tries 不变、不计失败、立即可被
// 领取。仅"因 shutdown 被取消"的任务走此路径；真实失败仍必须 Fail。
func (q *Queue) Release(ctx context.Context, t Task) error {
	if err := q.checkOpen(); err != nil {
		return err
	}
	k := q.eng.Keys()
	st, _, err := q.call(ctx, "release", []string{k.Lease, k.Ready, k.TaskPrefix + t.ID},
		t.Owner, t.Ver, t.ID)
	if err != nil {
		return err
	}
	if st != "OK" {
		if st == "ERR_FENCED" {
			q.metr.FencedTotal("fail")
		}
		return statusError(st, nil)
	}
	return nil
}

func (q *Queue) resultOf(status string) string {
	switch status {
	case "ERR_FENCED":
		return "fenced"
	case "ERR_STATE", "ERR_OWNER", "ERR_GONE", "ERR_DUP":
		return "stale"
	default:
		return "err"
	}
}

// errText 把 cause 转为可入存的单行文本。
func errText(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if len(s) > 1024 {
		s = s[:1024]
	}
	return s
}

// call 执行状态类脚本并把 {STATUS, ...} 拆为 status + fields。
func (q *Queue) call(ctx context.Context, script string, keys []string, args ...any) (string, []string, error) {
	reply, err := q.eng.Run(ctx, script, keys, args...)
	if err != nil {
		return "", nil, err
	}
	if len(reply) == 0 {
		return "", nil, fmt.Errorf("kiki: script %s: empty reply", script)
	}
	st, err := rdb.AsString(reply[0])
	if err != nil {
		return "", nil, err
	}
	fields := make([]string, 0, len(reply)-1)
	for _, f := range reply[1:] {
		s, err := rdb.AsString(f)
		if err != nil {
			return "", nil, err
		}
		fields = append(fields, s)
	}
	return st, fields, nil
}

// ---- 引擎级 sweep / promote / 维护操作（Scheduler 运行时与 kikictl 复用）----

// sweepResult 汇报一轮租约回收。
type sweepResult struct {
	Requeued     int
	DeadLettered int
	DLQIDs       []string // 本轮进 DLQ 的任务 id（触发 OnDLQ hook 用）
}

// sweep 执行一轮租约回收（转移 #9/#10）。幂等无主，多实例并发跑无害。
func (q *Queue) sweep(ctx context.Context, limit, maxRedeliveries int) (sweepResult, error) {
	var res sweepResult
	if err := q.checkOpen(); err != nil {
		return res, err
	}
	if limit < 1 {
		limit = 200
	}
	k := q.eng.Keys()
	reply, err := q.eng.Run(ctx, "sweep", []string{k.Lease, k.Ready, k.Sched, k.DLQ},
		k.TaskPrefix, limit, maxRedeliveries, q.retention.Milliseconds())
	if err != nil {
		return res, err
	}
	if len(reply) < 3 {
		return res, fmt.Errorf("kiki: sweep: malformed reply")
	}
	requeued, _ := strconv.Atoi(replyString(reply[1]))
	dlqd, _ := strconv.Atoi(replyString(reply[2]))
	res.Requeued, res.DeadLettered = requeued, dlqd
	for _, v := range reply[3:] {
		res.DLQIDs = append(res.DLQIDs, replyString(v))
	}
	if requeued > 0 {
		q.metr.SweepRequeuedTotal()
	}
	for range res.DLQIDs {
		q.metr.DLQTotal("sweep")
	}
	return res, nil
}

func replyString(v any) string {
	s, _ := v.(string)
	return s
}

// promote 执行一轮到期提升（转移 #3），返回提升的任务 id。
func (q *Queue) promote(ctx context.Context, limit int) ([]string, error) {
	if err := q.checkOpen(); err != nil {
		return nil, err
	}
	if limit < 1 {
		limit = 200
	}
	k := q.eng.Keys()
	reply, err := q.eng.Run(ctx, "promote", []string{k.Sched, k.Ready, k.TaskPrefix}, limit)
	if err != nil {
		return nil, err
	}
	if len(reply) < 2 {
		return nil, fmt.Errorf("kiki: promote: malformed reply")
	}
	ids := make([]string, 0, len(reply)-2)
	for _, v := range reply[2:] {
		ids = append(ids, replyString(v))
	}
	return ids, nil
}

// trimDLQ 把 DLQ Stream 裁剪到 MAXLEN ~ cap（近似裁剪，XTRIM）。
func (q *Queue) trimDLQ(ctx context.Context) error {
	if err := q.checkOpen(); err != nil {
		return err
	}
	return q.eng.Client().XTrimMaxLenApprox(ctx, q.eng.Keys().DLQ, q.dlqMaxLen, int64(q.dlqMaxLen/10+1)).Err()
}

// Stats 是队列深度快照（§10 可观测性）。
type Stats struct {
	ReadyDepth     int64
	SchedDepth     int64
	LeaseDepth     int64
	DLQLen         int64
	OldestReadyAge time.Duration // 0 表示 ready 为空
}

// Stats 从 ZCARD/ZRANGE/XLEN 推导队列深度。
func (q *Queue) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	if err := q.checkOpen(); err != nil {
		return s, err
	}
	c := q.eng.Client()
	k := q.eng.Keys()
	ready, err := c.ZCard(ctx, k.Ready).Result()
	if err != nil {
		return s, err
	}
	sched, err := c.ZCard(ctx, k.Sched).Result()
	if err != nil {
		return s, err
	}
	lease, err := c.ZCard(ctx, k.Lease).Result()
	if err != nil {
		return s, err
	}
	dlq, err := c.XLen(ctx, k.DLQ).Result()
	if err != nil {
		return s, err
	}
	if zs, err := c.ZRangeWithScores(ctx, k.Ready, 0, 0).Result(); err == nil && len(zs) > 0 {
		// score = pri×2^48 + ts_ms ⇒ ts_ms = score 低 48 位
		tsMs := int64(zs[0].Score) & ((1 << 48) - 1)
		if now, err := c.Time(ctx).Result(); err == nil {
			s.OldestReadyAge = now.Sub(time.UnixMilli(tsMs))
			if s.OldestReadyAge < 0 {
				s.OldestReadyAge = 0
			}
		}
	}
	s.ReadyDepth, s.SchedDepth, s.LeaseDepth, s.DLQLen = ready, sched, lease, dlq
	return s, nil
}

// DLQEntry 是 DLQ Stream 里的一条死信快照。
type DLQEntry struct {
	StreamID   string
	ID         string
	Payload    []byte
	Headers    map[string]string
	Err        string
	Tries      int
	MaxRetries int
	Pri        int
	Via        string // fail / sweep / abandon
	TS         time.Time
}

// ListDLQ 浏览死信快照（kikictl dlq ls / replay 的数据源）。
func (q *Queue) ListDLQ(ctx context.Context, count int64) ([]DLQEntry, error) {
	if err := q.checkOpen(); err != nil {
		return nil, err
	}
	msgs, err := q.eng.Client().XRangeN(ctx, q.eng.Keys().DLQ, "-", "+", count).Result()
	if err != nil {
		return nil, err
	}
	out := make([]DLQEntry, 0, len(msgs))
	for _, m := range msgs {
		e := DLQEntry{StreamID: m.ID}
		get := func(k string) string { s, _ := m.Values[k].(string); return s }
		e.ID = get("id")
		e.Payload = []byte(get("payload"))
		e.Err = get("err")
		e.Via = get("via")
		e.Tries, _ = strconv.Atoi(get("tries"))
		e.MaxRetries, _ = strconv.Atoi(get("max_retries"))
		e.Pri, _ = strconv.Atoi(get("pri"))
		if ts, err := strconv.ParseInt(get("ts"), 10, 64); err == nil {
			e.TS = time.UnixMilli(ts)
		}
		if h := get("headers"); h != "" {
			_ = json.Unmarshal([]byte(h), &e.Headers)
		}
		out = append(out, e)
	}
	return out, nil
}

// ReplayDLQ 把死信快照重新入队（新 tries 周期）。force=false 时保留期内的
// 残留 hash 会导致 ErrDup；force=true 经 replay.lua 的 DLQ 态守卫原子清除。
// 在途任务（非 DLQ 态）无论如何都会被拒绝——回放只针对已死信的任务。
func (q *Queue) ReplayDLQ(ctx context.Context, entries []DLQEntry, force bool) error {
	if err := q.checkOpen(); err != nil {
		return err
	}
	for _, e := range entries {
		if err := validateID(e.ID); err != nil {
			return err
		}
		hdr, err := json.Marshal(e.Headers)
		if err != nil {
			return fmt.Errorf("kiki: encode headers of %s: %w", e.ID, err)
		}
		k := q.eng.Keys()
		st, fields, err := q.call(ctx, "replay", []string{k.TaskPrefix + e.ID, k.Ready, k.Sched},
			e.ID, string(e.Payload), e.Pri, 0, e.MaxRetries, string(hdr), boolToFlag(force))
		if err != nil {
			return err
		}
		if st != "OK" {
			return statusError(st, fields)
		}
	}
	return nil
}

func boolToFlag(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// ---- Scheduler 附属 ----

// errLeaseExceeded 是 sweep 路径死信的固定原因（err=lease_exceeded）。
var errLeaseExceeded = errorString("lease_exceeded")

type errorString string

func (e errorString) Error() string { return string(e) }

// acquireScheduler 保证"首个 Worker 自动内嵌一个 Scheduler"：每个 Queue
// 只会被成功领取一次。返回 false 的 Worker 不再内嵌（多 Worker 共享队列）。
func (q *Queue) acquireScheduler() bool {
	return !q.schedulerUse.Swap(true)
}

// SweepOnce 手动执行一轮 sweep+promote（kikictl sweep / 救火模式）。
func (q *Queue) SweepOnce(ctx context.Context, limit int) (SweepStats, error) {
	var out SweepStats
	res, err := q.sweep(ctx, limit, q.maxRedeliveries)
	if err != nil {
		return out, err
	}
	out.Requeued = res.Requeued
	out.DeadLettered = res.DeadLettered
	if _, err := q.promote(ctx, limit); err != nil {
		return out, err
	}
	return out, nil
}

// SweepStats 汇报一轮手动维护的结果。
type SweepStats struct {
	Requeued     int
	DeadLettered int
}
