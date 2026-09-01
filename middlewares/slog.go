package middlewares

import (
	"context"
	"log/slog"

	"github.com/garfield-dev-team/kiki"
)

// Slog 输出结构化处理日志；task_id/tries/ver 恒在字段里（§7.1）。
func Slog(logger *slog.Logger) kiki.Middleware {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next kiki.Handler) kiki.Handler {
		return func(ctx context.Context, t kiki.Task) error {
			err := next(ctx, t)
			attrs := []any{"task_id", t.ID, "tries", t.Tries, "ver", t.Ver}
			if q, ok := kiki.QueueFromContext(ctx); ok {
				attrs = append(attrs, "queue", q)
			}
			switch {
			case err == nil:
				logger.Info("task processed", attrs...)
			case kiki.PermanentOf(err):
				logger.Warn("task non-retryable", append(attrs, "err", err)...)
			default:
				logger.Error("task failed", append(attrs, "err", err)...)
			}
			return err
		}
	}
}
