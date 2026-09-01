// Package middlewares 内建中间件（go-implementation.md §7.1）。
// 装配在 Terminator 之内：只影响 handler 的执行方式，无法拦截终结语义。
package middlewares

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/garfield-dev-team/kiki"
	"github.com/garfield-dev-team/kiki/internal/rdb"
)

// Dedup 实现消费侧幂等协议（design.md §7）：副作用执行前
// SET kiki:dedup:<task_id> NX EX ttl——抢不到说明别人做过（或正在做），
// 静默 ack（返回 nil，由 Terminator 正常 complete）。
//
// 去重键无 hash tag：不属于任何队列的状态机，cluster 自由分布。
// Redis 不可用时返回 err（当失败重试，保守正确）。
func Dedup(client redis.UniversalClient, ttl time.Duration) kiki.Middleware {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	host, err := os.Hostname()
	if err != nil {
		host = "?"
	}
	owner := fmt.Sprintf("%s:%d", host, os.Getpid())
	return func(next kiki.Handler) kiki.Handler {
		return func(ctx context.Context, t kiki.Task) error {
			ok, err := client.SetNX(ctx, rdb.DedupKey(t.ID), owner, ttl).Result()
			if err != nil {
				return fmt.Errorf("kiki/middlewares: dedup check: %w", err)
			}
			if !ok {
				return nil // 别人做过（或正在做）：直接放弃
			}
			return next(ctx, t)
		}
	}
}
