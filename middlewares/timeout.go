package middlewares

import (
	"context"
	"time"

	"github.com/garfield-dev-team/kiki"
)

// Timeout 限制单次处理时长。处理超时 ≠ 租约超时：前者触发 Fail（可重试），
// 后者触发重投——两条防线不可互相替代。
func Timeout(d time.Duration) kiki.Middleware {
	return func(next kiki.Handler) kiki.Handler {
		return func(ctx context.Context, t kiki.Task) error {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return next(ctx, t)
		}
	}
}
