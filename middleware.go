package kiki

import "context"

// Handler 是业务处理函数。返回 nil → complete；返回错误 → 指数退避重试；
// NonRetryable(err) → 跳过剩余重试直达 DLQ。
//
// handlerCtx 被取消意味着"租约可能已易主"：handler 应在下一个安全点停止——
// 即两次外部副作用之间；已经发出的外部调用让它自然结束，不要回滚也不要重试。
type Handler func(ctx context.Context, t Task) error

// Middleware 包装 Handler。装配顺序固定为
// [Terminator(最外层，SDK 私有), Recover(内联于 Terminator), 用户中间件...]：
// 用户中间件永远拿到"不会 panic 穿透"的保证，也永远无法拦截终结语义
// ——它们之下没有比 complete/fail 更低的一层。
type Middleware func(Handler) Handler

// Chain 从外到内依序包裹：Chain(a, b)(h) == a(b(h))。
func Chain(mws ...Middleware) Middleware {
	return func(next Handler) Handler {
		for i := len(mws) - 1; i >= 0; i-- {
			next = mws[i](next)
		}
		return next
	}
}
