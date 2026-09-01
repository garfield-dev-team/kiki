package kiki

import (
	"context"
	"errors"
	"sync"
	"time"
)

// startHeartbeat 拉起 per-task 租约保活 keeper（§4.3）。
//
// 关键语义：heartbeat 失败只做两件事——取消 handlerCtx（建议性中断）与计数。
// 绝不代替 handler 去 complete/fail：租约已丢，任何终结操作都会被脚本拒绝；
// 副作用是否落了一半，只有 handler 自己的幂等逻辑知道（design.md §7）。
//
// keeper 与 handlerCtx 同生命周期：fence 后自尽，process 返回时 ctx 取消后
// 退出，无泄漏路径。
func (w *Worker) startHeartbeat(ctx context.Context, t Task, fence func()) *sync.WaitGroup {
	var wg sync.WaitGroup
	if w.opts.HeartbeatInterval <= 0 {
		return &wg // 显式关闭（短任务场景）；零计数 Wait 立即返回
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk := time.NewTicker(w.opts.HeartbeatInterval)
		defer tk.Stop()
		fails := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				err := w.api.Heartbeat(ctx, t, w.opts.VisibilityTimeout)
				switch {
				case err == nil:
					fails = 0
				case errors.Is(err, ErrFenced), errors.Is(err, ErrGone):
					// ★ 租约已易主/任务已消亡：计数与告警已在 Queue op 层完成。
					fence()
					return
				default:
					fails++ // 网络抖动：下一 tick 重试
					if fails == 3 && w.hooks.OnHeartbeatLost != nil {
						w.hooks.OnHeartbeatLost(t, err)
					}
				}
			}
		}
	}()
	return &wg
}
