package middlewares

import (
	"context"
	"time"

	"github.com/garfield-dev-team/kiki"
)

// Metrics 记录 handler 直方图（kiki_handler_duration_seconds，queue 标签
// 取自 context carrier）。
func Metrics() kiki.Middleware {
	return func(next kiki.Handler) kiki.Handler {
		return func(ctx context.Context, t kiki.Task) error {
			start := time.Now()
			err := next(ctx, t)
			kiki.MetricsFromContext(ctx).HandlerDuration(time.Since(start).Seconds())
			return err
		}
	}
}
