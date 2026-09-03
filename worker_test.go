package kiki

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- queueAPI 替身：Worker 并发模型测试不依赖 Redis（AGENTS §6）----

type fakeAPI struct {
	mu       sync.Mutex
	reserved []Task
	failRes  FailResult
	failErr  error
	calls    map[string]*atomic.Int64
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{calls: map[string]*atomic.Int64{}}
}

func (f *fakeAPI) counter(name string) *atomic.Int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.calls[name]
	if !ok {
		c = &atomic.Int64{}
		f.calls[name] = c
	}
	return c
}

func (f *fakeAPI) ReserveFor(ctx context.Context, worker string, vis time.Duration, max int) ([]Task, error) {
	c := f.counter("reserve")
	c.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.reserved) == 0 {
		return nil, nil
	}
	t := f.reserved[0]
	f.reserved = f.reserved[1:]
	t.Owner = worker
	return []Task{t}, nil
}

func (f *fakeAPI) Complete(ctx context.Context, t Task, result string) error {
	f.counter("complete").Add(1)
	return nil
}

func (f *fakeAPI) Fail(ctx context.Context, t Task, cause error, backoff time.Duration) (FailResult, error) {
	f.counter("fail").Add(1)
	return f.failRes, f.failErr
}

func (f *fakeAPI) abandon(ctx context.Context, t Task, cause error) (FailResult, error) {
	f.counter("abandon").Add(1)
	return DeadLettered, nil
}

func (f *fakeAPI) Heartbeat(ctx context.Context, t Task, extend time.Duration) error {
	return nil
}

func (f *fakeAPI) Release(ctx context.Context, t Task) error {
	f.counter("release").Add(1)
	return nil
}

func newTestWorker(t *testing.T, api queueAPI, h Handler, mws ...Middleware) *Worker {
	t.Helper()
	q := &Queue{log: discardLogger(), metr: nopMetrics, visCap: 30 * time.Second, name: "t"}
	w := &Worker{
		eng:        q,
		api:        api,
		opts:       WorkerOptions{Concurrency: 4, VisibilityTimeout: time.Second, HeartbeatInterval: HeartbeatDisabled, PollInterval: time.Millisecond, ShutdownGrace: 2 * time.Second},
		log:        discardLogger(),
		metr:       nopMetrics,
		hooks:      Hooks{},
		backoff:    FixedBackoff(time.Millisecond),
		id:         "test-worker",
		rng:        rngForTest(),
		shutdownCh: make(chan struct{}),
		runDone:    make(chan struct{}),
	}
	w.chain = Chain(append([]Middleware{w.terminator()}, mws...)...)
	w.userHandler = h
	return w
}

// TestWorkerCompletesTasks — happy path：reserve → handler → complete。
// -race 下并发 4 slot × 多任务，同时验证 in-flight 账本与 goroutine 退出路径。
func TestWorkerCompletesTasks(t *testing.T) {
	api := newFakeAPI()
	var completed atomic.Int64
	for i := 0; i < 50; i++ {
		api.reserved = append(api.reserved, Task{ID: taskID(i), Tries: 1, Ver: 1})
	}
	w := newTestWorker(t, api, func(ctx context.Context, task Task) error {
		completed.Add(1)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = w.Run(ctx)
	}()
	// 等待全部处理完（fake 不回空前 tasks 不会耗尽）。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if completed.Load() == 50 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if completed.Load() != 50 {
		t.Fatalf("completed=%d want 50", completed.Load())
	}
	if api.counter("complete").Load() != 50 {
		t.Fatalf("complete calls=%d", api.counter("complete").Load())
	}
}

// TestWorkerFailRetriesAndDrainingRelease — 失败走 Fail；shutdown 取消中的
// handler 走 Release（判据 draining && ctx.Err()，二者缺一不可）。
func TestWorkerFailRetriesAndDrainingRelease(t *testing.T) {
	api := newFakeAPI()
	api.reserved = []Task{{ID: "t1", Tries: 1, Ver: 1}}
	entered := make(chan struct{})
	w := newTestWorker(t, api, func(ctx context.Context, task Task) error {
		close(entered)
		<-ctx.Done() // 等 shutdown 强制取消
		return ctx.Err()
	})
	go func() { _ = w.Run(context.Background()) }()
	<-entered

	// 在途任务卡在 handler；触发 shutdown ⇒ draining ⇒ 强取 ctx ⇒ Release。
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- w.Shutdown(context.Background()) }()
	// 给 grace 预算一小段，确认它没有立刻返回（在等 handler）。
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned before handler finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := <-shutdownDone; !errors.Is(err, context.DeadlineExceeded) {
		// §4.5：grace 内 handler 未自然结束 ⇒ WaitCtx 返回预算错误，
		// 下线流程仍完整走完（Release 已发生）。
		t.Fatalf("shutdown err = %v, want DeadlineExceeded", err)
	}
	if api.counter("release").Load() != 1 {
		t.Fatalf("release calls=%d want 1 (draining && ctx canceled ⇒ Release)", api.counter("release").Load())
	}
	if api.counter("fail").Load() != 0 {
		t.Fatalf("fail calls=%d want 0", api.counter("fail").Load())
	}
}

// TestWorkerPanicIsRetryableFailure — panic 不穿透 Terminator，转为可重试失败。
func TestWorkerPanicIsRetryableFailure(t *testing.T) {
	api := newFakeAPI()
	api.reserved = []Task{{ID: "t-panic", Tries: 1, Ver: 1}}
	w := newTestWorker(t, api, func(ctx context.Context, task Task) error {
		panic("boom")
	})
	w.hooks.OnPanic = func(task Task, r any, stack []byte) {}
	go func() { _ = w.Run(context.Background()) }()
	waitFor(t, func() bool { return api.counter("fail").Load() == 1 })
	if api.counter("complete").Load() != 0 {
		t.Fatal("panic must not complete")
	}
	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// TestShutdownIdempotent — Shutdown 幂等（§4.5），并发调用安全（-race 覆盖）。
func TestShutdownIdempotent(t *testing.T) {
	api := newFakeAPI() // 永远空队列：slot loop 空转
	w := newTestWorker(t, api, func(ctx context.Context, task Task) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- w.Run(ctx) }()
	time.Sleep(5 * time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Shutdown(context.Background()); err != nil {
				t.Errorf("concurrent shutdown: %v", err)
			}
		}()
	}
	wg.Wait()
	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("run err: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}
}

// TestFencedDropsTerminalAction — heartbeat keeper 判 fence 后 cancel handlerCtx；
// handler 因取消返回错误时，Terminator 命中 fence 分支，不再 complete/fail
// （§4.3 关键语义：租约已丢，任何终结操作都会被脚本拒绝）。
func TestFencedDropsTerminalAction(t *testing.T) {
	api := newFakeAPI()
	api.reserved = []Task{{ID: "t-fence", Tries: 1, Ver: 1}}
	returned := make(chan struct{})
	w := newTestWorker(t, api, func(ctx context.Context, task Task) error {
		<-ctx.Done() // handler 挂到被 fence cancel
		close(returned)
		return ctx.Err()
	})
	// 注入一个立刻 ErrFenced 的 Heartbeat：通过包装 fakeAPI。
	wrapped := &fencedAPI{fakeAPI: api}
	wrapped.hb = func(ctx context.Context, t Task, e time.Duration) error { return ErrFenced }
	w.api = wrapped
	w.opts.HeartbeatInterval = time.Millisecond
	go func() { _ = w.Run(context.Background()) }()
	select {
	case <-returned:
	case <-time.After(3 * time.Second):
		t.Fatal("handler not cancelled by fence in time")
	}
	if api.counter("complete").Load() != 0 || api.counter("fail").Load() != 0 {
		t.Fatalf("fenced task must not be completed/failed: complete=%d fail=%d",
			api.counter("complete").Load(), api.counter("fail").Load())
	}
	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

type fencedAPI struct {
	*fakeAPI
	hb func(ctx context.Context, t Task, e time.Duration) error
}

func (f *fencedAPI) Heartbeat(ctx context.Context, t Task, e time.Duration) error {
	return f.hb(ctx, t, e)
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met in time")
}
