package kiki

import (
	"context"
	"log/slog"
	"math/rand"
	"time"
)

// SchedulerOptions 是 Scheduler 配置。
type SchedulerOptions struct {
	// Interval 是 sweep+promote 巡检周期（+20% 抖动防齐步）。≈2s 的扫描
	// 误差是设计余量（design.md §11）——vis 不应被配置得连 2s 余量都容不下。
	Interval time.Duration
	// SweepLimit 是单轮回收上限（默认 200），防止单轮长脚本阻塞 Redis。
	SweepLimit int
	// MaxRedeliveries 是 lease_resets 毒丸上限（默认取队列值）。
	MaxRedeliveries int
	// TrimDLQ 是否每轮 XTRIM DLQ Stream（默认开）。
	TrimDLQ bool
	// OnDLQ 收到 sweep 路径死信时触发（fail/abandon 路径的 hook 在 Worker 侧）。
	OnDLQ func(t Task, via string, cause error)

	Logger *slog.Logger
}

// Scheduler 运行时：sweep + promote + XTRIM 循环（§5）。
//
// 无主幂等（design.md §5.6）：多实例并发跑、重复跑无害，故不做选主。
// 不自愈的代价要说清：若所有进程都关掉了 Scheduler 且无人接手，SCHEDULED/
// 超时租约会滞留——这是配置错误，靠 oldest_ready_age 与 lease_depth 告警
// 暴露，而不是靠造一个主节点来掩盖。
type Scheduler struct {
	q    *Queue
	opts SchedulerOptions
	log  *slog.Logger
	rng  *rand.Rand
}

// NewScheduler 构造独立 Scheduler（sidecar / kikictl sweep 场景）。
func (q *Queue) NewScheduler(opts SchedulerOptions) *Scheduler {
	log := opts.Logger
	if log == nil {
		log = q.log
	}
	interval := opts.Interval
	if interval <= 0 {
		interval = time.Second
	}
	opts.Interval = interval
	if opts.SweepLimit <= 0 {
		opts.SweepLimit = 200
	}
	if opts.MaxRedeliveries <= 0 {
		opts.MaxRedeliveries = q.maxRedeliveries
	}
	return &Scheduler{
		q:    q,
		opts: opts,
		log:  log.With("component", "scheduler"),
		rng:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Run 阻塞运行巡检循环；ctx 取消即退出。
func (s *Scheduler) Run(ctx context.Context) {
	for {
		interval := s.opts.Interval + time.Duration(s.rng.Float64()*float64(s.opts.Interval/5))
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			s.tick(ctx)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) {
	res, err := s.q.sweep(ctx, s.opts.SweepLimit, s.opts.MaxRedeliveries)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Error("sweep failed", "err", err)
		}
	} else if len(res.DLQIDs) > 0 && s.opts.OnDLQ != nil {
		// sweep 路径死信 err 恒为 lease_exceeded；hook 只负责"哪个"。
		for _, id := range res.DLQIDs {
			s.opts.OnDLQ(Task{ID: id}, "sweep", errLeaseExceeded)
		}
	}
	if _, err := s.q.promote(ctx, s.opts.SweepLimit); err != nil {
		if ctx.Err() == nil {
			s.log.Error("promote failed", "err", err)
		}
	}
	if s.opts.TrimDLQ {
		if err := s.q.trimDLQ(ctx); err != nil && ctx.Err() == nil {
			s.log.Error("dlq trim failed", "err", err)
		}
	}
	s.gaugeDepths(ctx)
}

// gaugeDepths 采集 depth / oldest_ready_age 指标（§8.1 前四行）。
func (s *Scheduler) gaugeDepths(ctx context.Context) {
	st, err := s.q.Stats(ctx)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Debug("stats failed", "err", err)
		}
		return
	}
	s.q.metr.GaugeDepth("ready", float64(st.ReadyDepth))
	s.q.metr.GaugeDepth("sched", float64(st.SchedDepth))
	s.q.metr.GaugeDepth("lease", float64(st.LeaseDepth))
	s.q.metr.GaugeOldestReady(st.OldestReadyAge.Seconds())
}
