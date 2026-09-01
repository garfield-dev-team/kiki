// Package metrics 提供 noop / prometheus 双实现（go-implementation.md §8.1）。
// 未提供注册器时全部指标为 noop。同一 Registerer 上的 vec 只注册一次，
// 多队列共享 vec、以 queue 标签区分。
package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Interface 是 Queue 引擎各路径打点的抽象。所有方法必须可并发调用。
type Interface interface {
	EnqueueTotal(result string)  // ok / dup / err
	ReserveTotal(result string)  // ok / empty / err
	CompleteTotal(result string) // ok / dup / fenced / err
	FailTotal(result string)     // retry / dlq / fenced / err
	FencedTotal(op string)       // hb / complete / fail — ★ 告警线
	DLQTotal(via string)         // fail / sweep / abandon
	SweepRequeuedTotal()
	GaugeDepth(kind string, v float64) // kind: ready / sched / lease
	GaugeOldestReady(seconds float64)
	HandlerDuration(seconds float64)
}

type noop struct{}

// New 返回 noop 实现（未提供 prometheus 注册器时的默认值）。
func NewNoop() Interface { return noop{} }

func (noop) EnqueueTotal(string)        {}
func (noop) ReserveTotal(string)        {}
func (noop) CompleteTotal(string)       {}
func (noop) FailTotal(string)           {}
func (noop) FencedTotal(string)         {}
func (noop) DLQTotal(string)            {}
func (noop) SweepRequeuedTotal()        {}
func (noop) GaugeDepth(string, float64) {}
func (noop) GaugeOldestReady(float64)   {}
func (noop) HandlerDuration(float64)    {}

// ---- prometheus 实现 ----

type vecSet struct {
	enqueue    *prometheus.CounterVec   // queue, result
	reserve    *prometheus.CounterVec   // queue, result
	complete   *prometheus.CounterVec   // queue, result
	fail       *prometheus.CounterVec   // queue, result
	fenced     *prometheus.CounterVec   // queue, op
	dlq        *prometheus.CounterVec   // queue, via
	swept      *prometheus.CounterVec   // queue
	depths     *prometheus.GaugeVec     // queue, kind
	oldest     *prometheus.GaugeVec     // queue
	handlerDur *prometheus.HistogramVec // queue
}

var (
	setsMu sync.Mutex
	sets   = map[prometheus.Registerer]*vecSet{}
)

func getSet(reg prometheus.Registerer) *vecSet {
	setsMu.Lock()
	defer setsMu.Unlock()
	if s, ok := sets[reg]; ok {
		return s
	}
	s := &vecSet{
		enqueue: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kiki_enqueue_total", Help: "Enqueue results."}, []string{"queue", "result"}),
		reserve: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kiki_reserve_total", Help: "Reserve results."}, []string{"queue", "result"}),
		complete: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kiki_complete_total", Help: "Complete results (ok/dup/fenced)."}, []string{"queue", "result"}),
		fail: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kiki_fail_total", Help: "Fail results (retry/dlq/fenced)."}, []string{"queue", "result"}),
		fenced: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kiki_fenced_total", Help: "Fencing rejections by op (hb/complete/fail). Sentinel for lease misjudgement."}, []string{"queue", "op"}),
		dlq: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kiki_dlq_total", Help: "Dead-lettered tasks by via path."}, []string{"queue", "via"}),
		swept: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kiki_sweep_requeued_total", Help: "Tasks requeued by lease sweep."}, []string{"queue"}),
		depths: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kiki_depth", Help: "Queue depth by kind (ready/sched/lease)."}, []string{"queue", "kind"}),
		oldest: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kiki_oldest_ready_seconds", Help: "Age of oldest ready task. Sustained growth = under-consumption or scheduler loss."}, []string{"queue"}),
		handlerDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "kiki_handler_duration_seconds", Help: "Handler duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"queue"}),
	}
	// AlreadyRegisteredError（如使用 DefaultRegisterer 且被并发初始化）视为成功。
	for _, c := range []prometheus.Collector{
		s.enqueue, s.reserve, s.complete, s.fail, s.fenced, s.dlq,
		s.swept, s.depths, s.oldest, s.handlerDur,
	} {
		_ = reg.Register(c)
	}
	sets[reg] = s
	return s
}

type prom struct {
	set   *vecSet
	queue string
}

// New 返回绑定队列名的 prometheus 实现；reg 为 nil 时返回 noop。
func New(reg prometheus.Registerer, queue string) Interface {
	if reg == nil {
		return noop{}
	}
	return prom{set: getSet(reg), queue: queue}
}

func (p prom) EnqueueTotal(result string)  { p.set.enqueue.WithLabelValues(p.queue, result).Inc() }
func (p prom) ReserveTotal(result string)  { p.set.reserve.WithLabelValues(p.queue, result).Inc() }
func (p prom) CompleteTotal(result string) { p.set.complete.WithLabelValues(p.queue, result).Inc() }
func (p prom) FailTotal(result string)     { p.set.fail.WithLabelValues(p.queue, result).Inc() }
func (p prom) FencedTotal(op string)       { p.set.fenced.WithLabelValues(p.queue, op).Inc() }
func (p prom) DLQTotal(via string)         { p.set.dlq.WithLabelValues(p.queue, via).Inc() }
func (p prom) SweepRequeuedTotal()         { p.set.swept.WithLabelValues(p.queue).Inc() }

func (p prom) GaugeDepth(kind string, v float64) {
	p.set.depths.WithLabelValues(p.queue, kind).Set(v)
}

func (p prom) GaugeOldestReady(seconds float64) {
	p.set.oldest.WithLabelValues(p.queue).Set(seconds)
}

func (p prom) HandlerDuration(seconds float64) {
	p.set.handlerDur.WithLabelValues(p.queue).Observe(seconds)
}
