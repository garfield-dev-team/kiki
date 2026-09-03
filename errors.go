package kiki

import (
	"errors"
	"fmt"
	"time"
)

// 哨兵错误：所有脚本 STATUS 收敛于此，调用方一律 errors.Is 判断
// （go-implementation.md §2.4 错误分类学）。禁止字符串匹配错误。
var (
	// ErrDup — enqueue/replay：任务 id 已存在（幂等语义，通常忽略）。
	ErrDup = errors.New("kiki: task id already exists")
	// ErrState — 状态机不允许该操作（stale 观察者，通常丢弃）。
	ErrState = errors.New("kiki: state machine rejects operation")
	// ErrOwner — 调用方不是租约持有者。
	ErrOwner = errors.New("kiki: caller is not the lease owner")
	// ErrFenced — fencing token 已过期，租约已易主。收到它必须停止一切后续
	// 动作并放弃：不重试、不当作普通错误上抛、不静默降级为成功。
	ErrFenced = errors.New("kiki: fencing token expired, lease moved on")
	// ErrGone — 任务 hash 已过保留期。
	ErrGone = errors.New("kiki: task gone (retention expired)")
	// ErrPayloadTooLarge — payload 超过队列上限（生产者侧修复）。
	ErrPayloadTooLarge = errors.New("kiki: payload exceeds queue limit")
	// ErrInvalidID — task id 未通过注入防线校验（§2.5）。
	ErrInvalidID = errors.New("kiki: invalid task id")
	// ErrInvalidName — 队列名非法（会破坏 hash tag / key 布局）。
	ErrInvalidName = errors.New("kiki: invalid queue name")
	// ErrInvalidArgument — 调用参数非法（负优先级、负重试数等）。
	ErrInvalidArgument = errors.New("kiki: invalid argument")
	// ErrNoHandler — Worker 未设置 handler 即 Run。
	ErrNoHandler = errors.New("kiki: worker has no handler")
	// ErrNoShard — 终结写携带的 Shard 句柄越界（Task 来自单队列、跨版本
	// 序列化丢失句柄、或手工构造 Task）。处置同级 ErrFenced：不重试、不吞
	// （docs/sharded-queue.md §5.3）。
	ErrNoShard = errors.New("kiki: task shard handle out of range")
	// ErrClosed — Queue 已关闭后仍被调用。
	ErrClosed = errors.New("kiki: queue closed")
)

// FailResult 由 fail/abandon 脚本的 RETRY/DLQ 状态映射，调用方据此决定是否告警。
type FailResult int

const (
	Retried      FailResult = iota // 已按退避重新入队
	DeadLettered                   // 已进死信（触发 OnDLQ hook）
)

func (r FailResult) String() string {
	switch r {
	case DeadLettered:
		return "DeadLettered"
	default:
		return "Retried"
	}
}

// statusError 把脚本返回的 STATUS 映射为哨兵错误；OK 家族不经过此函数。
func statusError(status string, fields []string) error {
	switch status {
	case "ERR_DUP":
		return fmt.Errorf("%w: %s", ErrDup, first(fields))
	case "ERR_STATE":
		return fmt.Errorf("%w: state=%s", ErrState, first(fields))
	case "ERR_OWNER":
		return ErrOwner
	case "ERR_FENCED":
		return ErrFenced
	case "ERR_GONE":
		return ErrGone
	default:
		return fmt.Errorf("kiki: unexpected script status %q", status)
	}
}

func first(fields []string) string {
	if len(fields) > 0 {
		return fields[0]
	}
	return ""
}

// ---- 失败包装器（供 handler 用，决定 fail 的走向，§2.4）----

type permanentError struct{ err error }

func (e *permanentError) Error() string { return "kiki: non-retryable: " + e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// NonRetryable 标记错误为不可重试：Terminator 将走 abandon 路径，
// 跳过剩余重试直接进 DLQ。
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// PermanentOf 解包判断错误是否携带 NonRetryable 标记。
func PermanentOf(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

type backoffError struct {
	err error
	d   time.Duration
}

func (e *backoffError) Error() string { return e.err.Error() }
func (e *backoffError) Unwrap() error { return e.err }

// WithBackoff 覆盖本轮失败的退避时长（优先于 BackoffPolicy）。
func WithBackoff(err error, d time.Duration) error {
	if err == nil {
		return nil
	}
	return &backoffError{err: err, d: d}
}

// backoffOf 解包 WithBackoff 标记。
func backoffOf(err error) (time.Duration, bool) {
	var b *backoffError
	if errors.As(err, &b) {
		return b.d, true
	}
	return 0, false
}
