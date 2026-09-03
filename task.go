package kiki

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/garfield-dev-team/kiki/internal/metrics"
)

// Task 是任务在 SDK 侧的完整形态。前半部分由生产者填写，reserve 后
// SDK 填充只读的租约字段，业务代码不得改写。
type Task struct {
	ID         string            // 生产者业务唯一键（§2.5 校验规则）
	Payload    []byte            // 不透明字节；SDK 提供 JSON 便捷构造
	Priority   int               // 0 最高；档位建议 ≤3（design.md §11 饥饿问题）
	MaxRetries int               // 0 → 取队列默认（首投之外的额外重试上限）
	Delay      time.Duration     // >0 → SCHEDULED
	Headers    map[string]string // traceparent 等旁路元数据，hash 内存单字段 JSON
	// Owner 是 reserve 时的租约持有者标识（SDK 填充）。
	// complete/fail/heartbeat/release 脚本用 owner+token 双重校验租约身份，
	// Task 必须携带 Owner 才能把两者回传给脚本（勘误 #8）。
	Owner string
	// ---- 以下字段 reserve 后由 SDK 填充，业务只读 ----
	Tries         int       // 第几次投递（从 1 起）
	Ver           int64     // ★ fencing token（design.md §4.3）
	LeaseDeadline time.Time // 当前租约到期时刻（Redis 服务器时钟）
	// Shard 是任务所在分片下标（SDK 填充，业务只读）。单队列恒为 -1；
	// ShardedQueue 的 complete/fail/heartbeat/release 经由它路由回原分片。
	// 它是路由句柄，刻意不内嵌进 id（决策记录：docs/sharded-queue.md §5.3）。
	Shard int

	// 刻意不设 Kind/Topic 字段：任务类型路由 = 队列名路由（§2.1）。

	// routeKey 是分片路由键（WithRouteKey 设置；空 = 按 ID 路由）。刻意不
	// 导出：路由是入队期决策，出了 SDK 边界没有意义；单队列上该选项被
	// 接受并忽略（docs/sharded-queue.md §5.2）。
	routeKey string
}

// EnqueueOption 定制入队行为，同时作用于 Enqueue 与 NewJSONTask。
type EnqueueOption func(*Task)

// WithPriority 设置优先级（0 最高；负值破坏 score 编码不变量，入队时报错）。
func WithPriority(pri int) EnqueueOption { return func(t *Task) { t.Priority = pri } }

// WithMaxRetries 覆盖队列默认重试上限（首投之外的额外重试次数）。
func WithMaxRetries(n int) EnqueueOption { return func(t *Task) { t.MaxRetries = n } }

// WithDelay 延迟投递（>0 → SCHEDULED）。
func WithDelay(d time.Duration) EnqueueOption { return func(t *Task) { t.Delay = d } }

// WithHeaders 设置旁路元数据（单字段 JSON 存入 task hash）。
func WithHeaders(h map[string]string) EnqueueOption { return func(t *Task) { t.Headers = h } }

// WithRouteKey 指定分片路由键：同一键恒落同一分片 ⇒ 分片内严格 FIFO +
// 严格优先级对同键成立。键的均匀性责任在调用方（选高基数字段，别选低基数）。
// 仅 ShardedQueue 感知；单队列接受并忽略。
func WithRouteKey(key string) EnqueueOption { return func(t *Task) { t.routeKey = key } }

// NewJSONTask 把任意值序列化为任务 payload。
func NewJSONTask(id string, v any, opts ...EnqueueOption) (Task, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return Task{}, fmt.Errorf("kiki: marshal payload: %w", err)
	}
	t := Task{ID: id, Payload: body}
	for _, o := range opts {
		o(&t)
	}
	return t, nil
}

// ---- ID / 队列名校验（key 注入防线，§2.5）----
//
// task id 会成为 key 名的一部分（{qk:q}:t:<id>）与 ZSET member；动态拼 key
// 换来的纪律成本在 API 边界一次付清。队列名同理（进入 hash tag，'{'/'}'
// 会破坏 {qk:q} 的 cluster slot 约束，'#' 保留给子分片命名约定）。

var (
	idRe    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:_.-]{0,127}$`)
	queueRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:_.#-]{0,127}$`)
)

func validateID(id string) error {
	if !idRe.MatchString(id) {
		return fmt.Errorf("%w: %q", ErrInvalidID, id)
	}
	return nil
}

func validateQueueName(name string) error {
	if !queueRe.MatchString(name) {
		return fmt.Errorf("%w: %q", ErrInvalidName, name)
	}
	return nil
}

// ---- Context carrier（§2.6）----
//
// handler 执行期间，ctx 内携带任务元数据与队列名；中间件借此取得
// kiki_handler_duration_seconds 等指标的 queue 标签，而不必感知 Worker。

type meta struct {
	queue   string
	task    Task
	fenced  *atomic.Bool
	metrics metrics.Interface
	hooks   *Hooks
}

func withMeta(ctx context.Context, m meta) context.Context {
	return context.WithValue(ctx, metaKey{}, m)
}

type metaKey struct{}

func metaFrom(ctx context.Context) meta {
	m, _ := ctx.Value(metaKey{}).(meta)
	return m
}

// TaskFromContext 在 handler 内取任务元数据（业务只读）。
func TaskFromContext(ctx context.Context) (Task, bool) {
	m := metaFrom(ctx)
	if m.task.ID == "" {
		return Task{}, false
	}
	return m.task, true
}

// QueueFromContext 返回任务所属队列名（中间件打指标用）。
func QueueFromContext(ctx context.Context) (string, bool) {
	m := metaFrom(ctx)
	if m.queue == "" {
		return "", false
	}
	return m.queue, true
}

// MetricsFromContext 返回 Worker 注入的指标句柄（ctx 外为 noop）。
// middlewares.Metrics 借此打 handler 直方图，构造时无需感知队列。
func MetricsFromContext(ctx context.Context) metrics.Interface {
	m := metaFrom(ctx)
	if m.metrics == nil {
		return metrics.NewNoop()
	}
	return m.metrics
}
