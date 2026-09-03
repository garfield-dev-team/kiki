package kiki

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/garfield-dev-team/kiki/internal/metrics"
)

// pollMaxInterval 是空转退避的上限（§4.2）。
const pollMaxInterval = 400 * time.Millisecond

// HeartbeatDisabled 显式关闭租约保活（短任务场景）。WorkerOptions.HeartbeatInterval
// 为 0 时取默认 vis/3，负值才表示关闭。
const HeartbeatDisabled time.Duration = -1

// Hooks 回答"哪个"：指标回答"多少"，hooks 接告警路由与采样日志（§8.3）。
type Hooks struct {
	OnFenced        func(op string, t Task)
	OnDLQ           func(t Task, via string, cause error)
	OnPanic         func(t Task, r any, stack []byte)
	OnHeartbeatLost func(t Task, err error)
}

// WorkerOptions 是 Worker 运行时配置。零值字段取默认值。
type WorkerOptions struct {
	// Concurrency 是 slot loop 数（默认 GOMAXPROCS）。
	Concurrency int
	// VisibilityTimeout 是本 worker 的 vis：可下调、不可超过队列上限
	// （上调会被钳制并告警）。禁止用调大 vis 代替心跳。
	VisibilityTimeout time.Duration
	// HeartbeatInterval 默认 vis/3；显式 HeartbeatDisabled 关闭（短任务场景）。
	HeartbeatInterval time.Duration
	// PollInterval 是 reserve 轮询起点（空转时自适应退避至 400ms）。
	PollInterval time.Duration
	// ShutdownGrace 是优雅下线在途处理等待的默认 deadline。
	ShutdownGrace time.Duration
	// Backoff 决定 fail 的退避（策略在客户端，§6.1）。
	Backoff BackoffPolicy
	// Middleware 用户中间件，装配在 Terminator（SDK 私有）之内。
	Middleware []Middleware
	// Hooks 见 Hooks 文档。
	Hooks Hooks
	// SchedulerInterval：默认首个 Worker 自动内嵌一个 Scheduler（1s + 抖动）；
	// 0 = 本 Worker 不内嵌（sidecar / kikictl sweep 接管）。
	SchedulerInterval time.Duration

	Logger *slog.Logger
}

// Worker 是消费侧运行时：slot loop × Concurrency，租约保活与终结写全部
// 汇聚到 Terminator。任务的状态迁移只发生在 Redis 脚本里。
type Worker struct {
	eng     engine   // 拥有者：*Queue 或 *ShardedQueue（Close 归它）
	api     queueAPI // 可注入替身（-race 单测）；生产恒为 eng
	opts    WorkerOptions
	id      string
	log     *slog.Logger
	hooks   Hooks
	metr    metrics.Interface
	backoff BackoffPolicy

	rng   *rand.Rand
	rngMu sync.Mutex

	chain       Middleware // 装配好的 [Terminator, 用户中间件]（§7）
	userHandler Handler
	runHandler  Handler // Run 时装配：chain(userHandler)

	draining       atomic.Bool
	inflight       inflightLedger
	handlerCancels sync.Map // map[*context.CancelFunc]struct{}，shutdown 强制取消用

	loopWG  sync.WaitGroup
	schedWG sync.WaitGroup
	running atomic.Bool

	dispatchCancel atomic.Pointer[context.CancelFunc]

	shutdownCh      chan struct{}
	shutdownSig     sync.Once
	shutdownMu      sync.Mutex
	shutdownStarted bool
	runDone         chan struct{}
	runDoneOnce     sync.Once
	runErrOnce      sync.Once
	runErr          error
}

// queueAPI 是 Worker 依赖的引擎操作面；*Queue 满足之。
// 抽出来是为了在 -race 单测中注入替身（AGENTS §6：并发代码必须自带用例）。
type queueAPI interface {
	ReserveFor(ctx context.Context, worker string, vis time.Duration, max int) ([]Task, error)
	Complete(ctx context.Context, t Task, result string) error
	Fail(ctx context.Context, t Task, cause error, backoff time.Duration) (FailResult, error)
	abandon(ctx context.Context, t Task, cause error) (FailResult, error)
	Heartbeat(ctx context.Context, t Task, extend time.Duration) error
	Release(ctx context.Context, t Task) error
}

// engine 是 Worker 运行时的完整依赖面：queueAPI + 归属信息 + Scheduler
// 内嵌点。*Queue 与 *ShardedQueue（docs/sharded-queue.md §7.3）都满足它，
// Worker 因此对"单队列 / 分片合并视图"无感知。
type engine interface {
	queueAPI
	Close() error
	engineName() string          // meta.queue 与日志标签（合并视图取逻辑名）
	engineVisCap() time.Duration // vis 钳制上限
	engineMetrics() metrics.Interface
	engineLogger() *slog.Logger
	acquireScheduler() bool // "首个 Worker 内嵌 Scheduler"的唯一授权点
	newScheduler(opts SchedulerOptions) schedulerRunner
}

// schedulerRunner 抽象单/多分片 Scheduler 的阻塞运行（ctx 取消退出）。
type schedulerRunner interface {
	Run(ctx context.Context)
}

var workerSeq atomic.Int64

func newWorkerID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "?"
	}
	return fmt.Sprintf("%s:%d:%d", host, os.Getpid(), workerSeq.Add(1))
}

// NewWorker 构造 Worker（先 Handle 再 Run）。
func (q *Queue) NewWorker(opts WorkerOptions) *Worker {
	return newWorkerFor(q, opts)
}

// newWorkerFor 是两个构造器（单队列 / ShardedQueue）共享的装配点：默认值
// 解析必须写回 w.opts（slot loop / heartbeat / shutdown 都读它）。
func newWorkerFor(eng engine, opts WorkerOptions) *Worker {
	log := opts.Logger
	if log == nil {
		log = eng.engineLogger()
	}
	visCap := eng.engineVisCap()
	vis := opts.VisibilityTimeout
	if vis <= 0 {
		vis = visCap
	}
	hb := opts.HeartbeatInterval
	if hb == 0 {
		hb = vis / 3 // 默认 vis/3：丢一次心跳不至于超时，连续两次丢失应告警
	}
	poll := opts.PollInterval
	if poll <= 0 {
		poll = 100 * time.Millisecond
	}
	grace := opts.ShutdownGrace
	if grace <= 0 {
		grace = 30 * time.Second
	}
	backoff := opts.Backoff
	if backoff == nil {
		backoff = ExponentialBackoff(time.Second, 60*time.Second, 0.5)
	}
	conc := opts.Concurrency
	if conc <= 0 {
		conc = max(1, runtime.GOMAXPROCS(0))
	}
	id := newWorkerID()
	w := &Worker{
		eng:        eng,
		api:        eng,
		opts:       opts,
		id:         id,
		log:        log.With("worker", id),
		hooks:      opts.Hooks,
		metr:       eng.engineMetrics(),
		backoff:    backoff,
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
		shutdownCh: make(chan struct{}),
		runDone:    make(chan struct{}),
	}
	if w.opts.VisibilityTimeout <= 0 {
		w.opts.VisibilityTimeout = vis
	}
	// vis 高于队列上限则钳制：worker 可下调不可上调（§9）。
	if w.opts.VisibilityTimeout > visCap {
		log.Warn("worker visibility timeout clamped to queue cap; use heartbeat for long tasks",
			"requested", w.opts.VisibilityTimeout.String(), "cap", visCap.String())
		w.opts.VisibilityTimeout = visCap
	}
	w.opts.HeartbeatInterval = hb
	w.opts.PollInterval = poll
	w.opts.ShutdownGrace = grace
	w.opts.Concurrency = conc
	// 装配顺序固定：[Terminator(最外层，SDK 私有), 用户中间件...]（§7）。
	w.chain = Chain(append([]Middleware{w.terminator()}, opts.Middleware...)...)
	return w
}

// Handle 设置业务处理函数（必须先于 Run 调用）。
func (w *Worker) Handle(h Handler) { w.userHandler = h }

// Run 拉起 slot loop 并阻塞：ctx 取消或 Shutdown 调用后开始优雅下线
// （draining → 等待在途 → 强制取消 → Release 收尾）。
func (w *Worker) Run(ctx context.Context) error {
	if w.userHandler == nil {
		return ErrNoHandler
	}
	w.running.Store(true)
	w.runHandler = w.chain(w.userHandler)
	dctx, dcancel := context.WithCancel(ctx)
	w.dispatchCancel.Store(&dcancel)
	for i := 0; i < w.opts.Concurrency; i++ {
		w.loopWG.Add(1)
		go w.slotLoop(dctx)
	}
	// 首个 Worker 自动内嵌 Scheduler（§5）；后到的 Worker 不再内嵌。
	// 分片合并视图下这一授权点一次拉起全部 N 个分片级 scheduler。
	if w.opts.SchedulerInterval != 0 && w.eng.acquireScheduler() {
		sched := w.eng.newScheduler(SchedulerOptions{
			Interval: w.opts.SchedulerInterval,
			OnDLQ:    w.hooks.OnDLQ,
		})
		w.schedWG.Add(1)
		go func() {
			defer w.schedWG.Done()
			sched.Run(dctx)
		}()
	}
	defer dcancel()
	select {
	case <-ctx.Done():
	case <-w.shutdownCh:
	}
	// 终结写必须穿透已取消的 ctx；grace 等待用独立预算。
	graceCtx, cancelGrace := context.WithTimeout(context.WithoutCancel(ctx), w.opts.ShutdownGrace)
	defer cancelGrace()
	w.driveShutdown(graceCtx)
	return w.runErr
}

// Shutdown 优雅下线（幂等）。err 非 nil 仅表示等待在途处理时超过了 ctx
// 预算——下线流程仍会完整走完（强制取消 → Release 收尾）。
func (w *Worker) Shutdown(ctx context.Context) error {
	w.shutdownSig.Do(func() { close(w.shutdownCh) })
	if ctx.Done() == nil { // 未设置 deadline 时用默认 grace，避免无限等
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), w.opts.ShutdownGrace)
		defer cancel()
	}
	w.driveShutdown(ctx)
	return w.runErr
}

// Close = Shutdown(ShutdownGrace) + Queue.Close。
func (w *Worker) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), w.opts.ShutdownGrace)
	defer cancel()
	err := w.Shutdown(ctx)
	_ = w.eng.Close()
	return err
}

// driveShutdown 执行 §4.5 的顺序——顺序即正确性。并发调用以先到者的
// ctx 预算为准，后者等待收尾完成。
func (w *Worker) driveShutdown(graceCtx context.Context) {
	w.shutdownMu.Lock()
	if w.shutdownStarted {
		w.shutdownMu.Unlock()
		<-w.runDone
		return
	}
	w.shutdownStarted = true
	w.shutdownMu.Unlock()

	w.draining.Store(true) // 1. Terminator 切换语义
	if c := w.dispatchCancel.Load(); c != nil {
		(*c)() // 2. slot loop 停止 reserve（在途 reserve 自然结束）
	}
	err := w.inflight.WaitCtx(graceCtx) // 3. 等待在途 handler 至 grace deadline
	w.forceCancelHandlers()             // 4. 到点强制 cancel 所有 handlerCtx
	w.inflight.Wait()                   // 5. Terminator 以 Release 收尾（tries 不变）
	w.loopWG.Wait()
	w.schedWG.Wait()
	w.runErrOnce.Do(func() { w.runErr = err })
	w.runDoneOnce.Do(func() { close(w.runDone) })
}

// slotLoop 是 §4.2 的 reserve-处理内联循环。
func (w *Worker) slotLoop(ctx context.Context) {
	defer w.loopWG.Done()
	idle := w.opts.PollInterval
	for {
		if ctx.Err() != nil {
			return
		}
		tasks, err := w.api.ReserveFor(ctx, w.id, w.opts.VisibilityTimeout, 1)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, ErrClosed) {
				return
			}
			w.metr.ReserveTotal("err")
			w.log.Error("reserve failed", "err", err)
			w.sleep(ctx, idle) // 网络错误：退避轮询，不退出
			continue
		}
		if len(tasks) == 0 {
			w.metr.ReserveTotal("empty")
			idle = min(pollMaxInterval, idle*2) // 自适应空转退避
			w.sleep(ctx, idle)
			continue
		}
		w.metr.ReserveTotal("ok")
		idle = w.opts.PollInterval
		w.process(ctx, tasks[0]) // 内联；Prefetch=1，崩溃未处理窗口最小化
	}
}

func (w *Worker) sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// process 运行单个任务。handlerCtx 刻意脱离 dispatch ctx 的取消链
// （WithoutCancel 保留取值、丢弃取消）：shutdown 第 2 步只停 slot loop，
// 在途 handler 必须继续跑到 grace deadline 或自然结束，第 4 步才强制取消；
// 终结路径（fence / 强制取消 / process 返回）经 cancelHandler 显式触达。
func (w *Worker) process(slotCtx context.Context, t Task) {
	w.inflight.Add()
	defer w.inflight.Done()

	handlerCtx, cancelHandler := context.WithCancel(context.WithoutCancel(slotCtx))
	w.trackHandler(&cancelHandler)
	defer w.untrackHandler(&cancelHandler)

	fenced := &atomic.Bool{}
	ctx := withMeta(handlerCtx, meta{
		queue:   w.eng.engineName(),
		task:    t,
		fenced:  fenced,
		metrics: w.metr,
		hooks:   &w.hooks,
	})

	keeperDone := w.startHeartbeat(handlerCtx, t, func() {
		fenced.Store(true) // 建议性中断：取消 handlerCtx，绝不代替 handler 去 complete/fail
		cancelHandler()
	})
	// defer 是 LIFO：先 cancelHandler（终结 keeper），再 join keeper——
	// 顺序颠倒会死锁（keeper 在等 ctx 取消，Wait 在等 keeper 退出）。
	defer func() {
		cancelHandler()
		keeperDone.Wait()
	}()

	w.runHandler(ctx, t)
}

func (w *Worker) trackHandler(cancel *context.CancelFunc)   { w.handlerCancels.Store(cancel, struct{}{}) }
func (w *Worker) untrackHandler(cancel *context.CancelFunc) { w.handlerCancels.Delete(cancel) }

func (w *Worker) forceCancelHandlers() {
	w.handlerCancels.Range(func(key, _ any) bool {
		if c, ok := key.(*context.CancelFunc); ok && c != nil {
			(*c)()
		}
		return true
	})
}

// ---- Terminator：complete/fail/abandon/release 的唯一入口（§4.4）----

func (w *Worker) terminator() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, t Task) error {
			herr := w.safeNext(next, ctx, t)
			switch {
			case herr == nil:
				w.finishComplete(ctx, t)
			case fencedFrom(ctx):
				// 租约已易主：不再 complete/fail——任何终结操作都会被脚本拒绝；
				// 重投由 sweep 体系负责。副作用是否落了一半，只有 handler 自己
				// 的幂等逻辑知道。
				w.log.Warn("lease fenced during processing; dropping terminal action",
					"id", t.ID, "tries", t.Tries)
				if w.hooks.OnFenced != nil {
					w.hooks.OnFenced("handler", t)
				}
			case PermanentOf(herr):
				w.finishAbandon(ctx, t, herr)
			case w.draining.Load() && ctx.Err() != nil:
				// 仅"因 shutdown 被取消"走 Release；二者缺一不可——否则 DB 超时
				// 会伪装成下线，任务无限 release 循环。
				rerr := w.api.Release(context.WithoutCancel(ctx), t)
				w.logFinish("release", t, rerr)
			default:
				backoff := w.nextBackoff(t.Tries)
				if d, ok := backoffOf(herr); ok {
					backoff = d
				}
				res, ferr := w.api.Fail(context.WithoutCancel(ctx), t, herr, backoff)
				w.logFinish("fail", t, ferr)
				if ferr == nil && res == DeadLettered && w.hooks.OnDLQ != nil {
					w.hooks.OnDLQ(t, "fail", herr)
				}
			}
			return nil // 槽位永远继续
		}
	}
}

// safeNext 恢复 handler panic 并转为可重试失败（栈随任务存档）——
// panic 错误进入与普通错误相同的终结分类，而不是绕过它。
func (w *Worker) safeNext(next Handler, ctx context.Context, t Task) (herr error) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			w.log.Error("handler panic", "id", t.ID, "panic", r)
			if w.hooks.OnPanic != nil {
				w.hooks.OnPanic(t, r, stack)
			}
			herr = fmt.Errorf("panic: %v", r)
		}
	}()
	return next(ctx, t)
}

func fencedFrom(ctx context.Context) bool {
	m := metaFrom(ctx)
	return m.fenced != nil && m.fenced.Load()
}

func (w *Worker) finishComplete(ctx context.Context, t Task) {
	err := w.api.Complete(context.WithoutCancel(ctx), t, "")
	w.logFinish("complete", t, err)
}

func (w *Worker) finishAbandon(ctx context.Context, t Task, cause error) {
	res, err := w.api.abandon(context.WithoutCancel(ctx), t, cause)
	w.logFinish("abandon", t, err)
	if err == nil && res == DeadLettered && w.hooks.OnDLQ != nil {
		w.hooks.OnDLQ(t, "abandon", cause)
	}
}

// logFinish 按 §4.4 结果处理矩阵记录日志（指标已在 Queue op 层计数；
// 此处只区分日志级别：fenced=warn，stale=debug，其余不刷屏）。
func (w *Worker) logFinish(op string, t Task, err error) {
	switch {
	case err == nil:
	case errors.Is(err, ErrFenced):
		w.log.Warn("terminal op fenced; task already re-owned by queue",
			"op", op, "id", t.ID, "tries", t.Tries)
		if w.hooks.OnFenced != nil {
			w.hooks.OnFenced(op, t)
		}
	case errors.Is(err, ErrState), errors.Is(err, ErrOwner), errors.Is(err, ErrGone):
		// 终态保留期后过期属正常；stale 观察者丢弃。
		w.log.Debug("terminal op rejected by state machine", "op", op, "id", t.ID, "err", err)
	default:
		w.log.Error("terminal op failed", "op", op, "id", t.ID, "err", err)
	}
}

func (w *Worker) nextBackoff(tries int) time.Duration {
	w.rngMu.Lock()
	defer w.rngMu.Unlock()
	return w.backoff.Next(tries, w.rng)
}

// ---- in-flight 账本（§4.6）----
//
// 只服务 shutdown，不参与正确性（正确性全在 Redis 侧状态机）。

type inflightLedger struct {
	wg sync.WaitGroup
}

func (f *inflightLedger) Add()  { f.wg.Add(1) }
func (f *inflightLedger) Done() { f.wg.Done() }
func (f *inflightLedger) Wait() { f.wg.Wait() }

// WaitCtx 受 ctx 约束地等待清零；超时后监视 goroutine 仍会存活到账本清零
// （账本只服务 shutdown，泄漏有界）。
func (f *inflightLedger) WaitCtx(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		f.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
