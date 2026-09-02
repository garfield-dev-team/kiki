// Agent Sandbox（Manus 类产品）场景演示——按 agent-sandbox-task-queue.md 的指引，
// 把 kiki 应用到"会话 ↔ 容器沙箱"的资源租用场景（sandbox API 用 mock，生产替换为 Docker/K8s 调用）。
//
// 建模转换（文档开头的关键洞见）：沙箱不是任务，是"被租用的资源"。
//
//	一个资源池 + 三条事件队列：
//	  sandbox-pool  资源池：warm 容器当被 reserve 的对象（回收/心跳/fencing 全套机制作用其上）
//	  sandbox-req   用户请求（priority = 会员等级；队列天然是等待室）
//	  sandbox-prov  容器供给（delay 错峰预热；供给失败走 fail 退避 → DLQ）
//	  sandbox-reset 回收清理（sanitize 后新一代回池——用队列自己的毒丸机制保证清理可靠性）
//
//	唯一新写的脚本是 bind.lua（Dispatcher 原子配对，见同目录）——"在原状态机上做加法，不改地基"。
//
// 运行：go run ./example/sandbox
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/garfield-dev-team/kiki"
	"github.com/garfield-dev-team/kiki/example/internal/demo"
)

//go:embed bind.lua
var bindSrc string

const (
	sessionVis       = 400 * time.Millisecond // 会话租期（生产：vis=60s，网关 20s 心跳）
	gatewayHeartbeat = 100 * time.Millisecond
	provColdStart    = 120 * time.Millisecond // mock 冷启动（生产 2–60s，期间心跳保活）
)

// ---- mock sandbox API（生产实现 = 调 Docker/K8s API）----

type sandboxAPI struct {
	mu       sync.Mutex
	attempts map[string]int
	running  map[string]bool
}

func newSandboxAPI() *sandboxAPI {
	return &sandboxAPI{attempts: map[string]int{}, running: map[string]bool{}}
}

// Provision 拉起容器；failNTimes>0 时前 N 次调用故意失败（演示供给失败重试）。
func (a *sandboxAPI) Provision(id string, failNTimes int) error {
	a.mu.Lock()
	a.attempts[id]++
	n := a.attempts[id]
	a.mu.Unlock()
	if n <= failNTimes {
		return fmt.Errorf("image pull timeout (attempt %d)", n)
	}
	time.Sleep(provColdStart)
	a.mu.Lock()
	a.running[id] = true
	a.mu.Unlock()
	return nil
}

func (a *sandboxAPI) Kill(id string) {
	a.mu.Lock()
	a.running[id] = false
	a.mu.Unlock()
	fmt.Printf("  [docker] 强杀容器 %s（资源释放）\n", id)
}

func (a *sandboxAPI) Sanitize(id string) {
	time.Sleep(60 * time.Millisecond)
	a.mu.Lock()
	a.running[id] = false // reset 流程含销毁重建/重置，简化为原地清理
	a.mu.Unlock()
}

func (a *sandboxAPI) IsRunning(id string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.running[id]
}

// ---- 任务 payload ----

type containerSpec struct {
	Container string `json:"container"`
	Addr      string `json:"addr"`
	FailTimes int    `json:"fail_times,omitempty"`
}

type resetSpec struct {
	Container string `json:"container"`
	Addr      string `json:"addr"`
	Gen       int    `json:"gen"`
}

type requestSpec struct {
	User string `json:"user"`
	Tier int    `json:"tier"` // 会员等级 = priority，0 最高
}

type session struct {
	id, reqID, container, addr string
	token                      int64
	stop                       chan struct{}
}

// startGateway 模拟网关会话：定时心跳续租（生产：agent 每次 API 调用也顺带续租，
// "用户还在干活，容器就留着"）。租约丢失 → 打印 410（ERR_FENCED = 安全边界）。
func startGateway(q *kiki.Queue, s *session) {
	tk := time.NewTicker(gatewayHeartbeat)
	go func() {
		defer tk.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-tk.C:
				err := q.Heartbeat(context.WithoutCancel(context.Background()),
					kiki.Task{ID: s.container, Owner: s.id, Ver: s.token}, sessionVis)
				switch {
				case err == nil:
					fmt.Printf("  [gateway] ♥ %s 续租 %s（token=%d）\n", s.id, s.container, s.token)
				case errors.Is(err, kiki.ErrFenced):
					fmt.Printf("  [gateway] ✗ %s 的请求撞上 token 校验 → ERR_FENCED → 返回 410 Gone（僵尸会话碰不到已易主的沙箱）\n", s.id)
					return
				default:
					return // 演示网络噪音直接放弃
				}
			}
		}
	}()
}

func endSession(q *kiki.Queue, s *session) {
	close(s.stop)
	// 正常结束：complete 容器租约（回收清理走 reset 队列），complete 请求（等待室出清）
	if err := q.Complete(context.Background(),
		kiki.Task{ID: s.container, Owner: s.id, Ver: s.token}, ""); err != nil {
		demo.Fatal("complete 容器租约", err)
	}
	_ = q.Complete(context.Background(), kiki.Task{ID: s.reqID, Owner: s.id}, "")
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		demo.Fatal("marshal", err)
	}
	return b
}

func main() {
	ctx := context.Background()
	api := newSandboxAPI()
	client := demo.Connect()

	// ---- 一个资源池 + 三条事件队列（文档 §二）----
	pool := demo.NewQueue(client, demo.Unique("sandbox-pool"), func(o *kiki.QueueOptions) {
		o.MaxRedeliveries = 1 // 坏容器毒丸上限：反复被动回收 ⇒ 停止流转进 DLQ
	})
	reqQ := demo.NewQueue(client, demo.Unique("sandbox-req"), nil)
	prov := demo.NewQueue(client, demo.Unique("sandbox-prov"), func(o *kiki.QueueOptions) {
		o.MaxRetries = 2 // 供给失败毒丸上限
	})
	resetQ := demo.NewQueue(client, demo.Unique("sandbox-reset"), nil)
	fmt.Printf("资源池与队列：%s | %s | %s | %s\n", pool.Name(), reqQ.Name(), prov.Name(), resetQ.Name())
	stopPoolSweep := demo.RunScheduler(pool) // ★ 僵尸容器回收器（无主幂等，成本控制核心）
	stopProvSweep := demo.RunScheduler(prov) // 延迟预热任务的 promote
	defer func() { stopPoolSweep(); stopProvSweep() }()

	// ---- Provisioner worker：reserve → 调 Docker API（mock）→ complete，容器进池 ----
	pw := prov.NewWorker(kiki.WorkerOptions{
		Concurrency:       2,
		VisibilityTimeout: 5 * time.Second,
		Backoff:           kiki.FixedBackoff(40 * time.Millisecond), // 演示加速；生产默认 1s/60s/±0.5
		PollInterval:      15 * time.Millisecond,
	})
	pw.Handle(func(ctx context.Context, t kiki.Task) error {
		var spec containerSpec
		if err := json.Unmarshal(t.Payload, &spec); err != nil {
			return kiki.NonRetryable(err)
		}
		fmt.Printf("  [provisioner] 拉起容器 %s（mock 冷启动 %v，第 %d 次尝试）\n",
			spec.Container, provColdStart, t.Tries)
		if err := api.Provision(spec.Container, spec.FailTimes); err != nil {
			return err // fail → 退避重试；超 MaxRetries → DLQ（镜像拉取失败待人工）
		}
		return pool.Enqueue(ctx, spec.Container, mustJSON(containerSpec{Container: spec.Container, Addr: spec.Addr}))
	})
	go func() { _ = pw.Run(ctx) }()

	// ---- Reset worker：sanitize 后新一代回池（回收 ≠ 直接复用，文档 §四.3）----
	rw := resetQ.NewWorker(kiki.WorkerOptions{PollInterval: 15 * time.Millisecond})
	rw.Handle(func(ctx context.Context, t kiki.Task) error {
		var spec resetSpec
		if err := json.Unmarshal(t.Payload, &spec); err != nil {
			return kiki.NonRetryable(err)
		}
		api.Sanitize(spec.Container)
		next := fmt.Sprintf("%s-g%d", spec.Container, spec.Gen+1)
		if err := pool.Enqueue(ctx, next, mustJSON(containerSpec{Container: next, Addr: spec.Addr})); err != nil {
			return err // reset 失败进 DLQ，而不是把脏容器发给别人——用队列保证清理可靠性
		}
		fmt.Printf("  [reset] %s sanitize 完成 → 新一代 %s 回池复用\n", spec.Container, next)
		return nil
	})
	go func() { _ = rw.Run(ctx) }()

	// ---- Dispatcher：bind.lua 原子配对（等待室 × 资源池）----
	bind := redis.NewScript(bindSrc)
	bindKeys := []string{
		"{qk:" + reqQ.Name() + "}:ready",
		"{qk:" + pool.Name() + "}:ready",
		"{qk:" + pool.Name() + "}:lease",
		"{qk:" + pool.Name() + "}:bind",
	}
	var dispatchSeq int
	dispatchOnce := func() *session {
		dispatchSeq++
		sessID := fmt.Sprintf("sess-%d", dispatchSeq)
		res, err := bind.Run(ctx, client, bindKeys,
			sessID, sessionVis.Milliseconds(),
			"{qk:"+reqQ.Name()+"}:t:", "{qk:"+pool.Name()+"}:t:").Result()
		if err != nil {
			demo.Fatal("bind.lua", err)
		}
		row := res.([]interface{})
		if row[0] == "EMPTY" {
			return nil // 池空或无请求：原地不动
		}
		s := &session{
			id: sessID, reqID: row[1].(string), container: row[2].(string),
			addr: row[3].(string), token: atoi(string(row[4].(string))),
			stop: make(chan struct{}),
		}
		var req requestSpec
		_ = json.Unmarshal([]byte(demo.Field(reqQ, client, s.reqID, "payload")), &req)
		fmt.Printf("  [dispatcher] 原子配对：%s ⇔ 容器 %s@%s（token=%d）——用户 %s（tier=%d）\n",
			s.id, s.container, s.addr, s.token, req.User, req.Tier)
		return s
	}

	// ================= 第 1 章：池水位低 → 容器供给（延迟预热 / 失败重试 / 供给毒丸）=================
	fmt.Println("\n—— 第 1 章：容器供给（Provisioner）——")
	provTask := func(id string, fail int, delay time.Duration) {
		var err error
		spec := containerSpec{Container: id, Addr: fmt.Sprintf("10.0.0.%d:2375", len(id))}
		if delay > 0 {
			err = prov.EnqueueIn(ctx, id, mustJSON(spec), delay)
		} else {
			err = prov.Enqueue(ctx, id, mustJSON(spec))
		}
		if err != nil {
			demo.Fatal("enqueue prov", err)
		}
	}
	provTask("sbx-1", 0, 0)                    // 正常供给
	provTask("sbx-2", 1, 0)                    // 首次拉镜像失败 → 退避重试
	provTask("sbx-3", 0, 300*time.Millisecond) // 错峰预热：延迟任务（预测早高峰提前排）
	provTask("sbx-bad", 99, 0)                 // 毒丸：镜像永远拉不下来
	demo.Await("warm 容器进池（sbx-1/sbx-2）", 5*time.Second, func() bool {
		n, _ := client.ZCard(ctx, "{qk:"+pool.Name()+"}:ready").Result()
		return n >= 2
	})
	fmt.Println("  [dispatcher] 池水位低于目标 → 已排供给任务（sbx-3 延迟 300ms = 错峰预热）")
	entries, _ := prov.ListDLQ(ctx, 10)
	if len(entries) == 1 {
		demo.Check(entries[0].Via == "fail" && entries[0].ID == "sbx-bad",
			"供给毒丸：sbx-bad 镜像反复拉取失败 → tries=%d > MaxRetries=2 → DLQ 待人工", entries[0].Tries)
	}

	// ================= 第 2 章：请求排队 + 优先级配对（等待室）=================
	fmt.Println("\n—— 第 2 章：请求排队 + bind.lua 原子配对（priority=会员等级）——")
	if err := reqQ.Enqueue(ctx, "req-free", mustJSON(requestSpec{User: "free", Tier: 1})); err != nil {
		demo.Fatal("enqueue req", err)
	}
	if err := reqQ.Enqueue(ctx, "req-vip", mustJSON(requestSpec{User: "vip", Tier: 0})); err != nil {
		demo.Fatal("enqueue req", err)
	}
	vip := dispatchOnce()
	demo.Check(vip != nil && vip.reqID == "req-vip",
		"会员优先：VIP（tier=0）先于 free（tier=1）拿到容器（ZPOPMIN 队头，等待室天然保序）")
	free := dispatchOnce()
	demo.Check(free != nil && free.reqID == "req-free" && vip.container != free.container,
		"分配互斥：两个会话各得一个容器，绝无同一个容器被二次分配")

	// ================= 第 3 章：会话租约（心跳挂活动上）+ 正常结束 → reset 回池 =================
	fmt.Println("\n—— 第 3 章：会话租约与优雅回收 ——")
	startGateway(pool, vip) // 网关心跳；生产中 agent 每次 API 调用也续租
	time.Sleep(250 * time.Millisecond)
	fmt.Println("  [session] vip 用户干完活，正常结束")
	endSession(pool, vip)
	if err := resetQ.Enqueue(ctx, "reset-sbx-1", mustJSON(resetSpec{Container: "sbx-1", Addr: vip.addr, Gen: 1})); err != nil {
		demo.Fatal("enqueue reset", err)
	}
	demo.Await("sanitize 后新一代回池", 5*time.Second, func() bool {
		n, _ := client.ZCard(ctx, "{qk:"+pool.Name()+"}:ready").Result()
		return n >= 3 // sbx-3 + sbx-1-g2 + 剩余一个
	})
	demo.Check(demo.State(pool, client, "sbx-1-g2") == "READY",
		"回收 ≠ 直接复用：sanitize 后以新一代任务（sbx-1-g2）回池，脏容器绝不发给下一个用户")

	// ================= 第 4 章：僵尸回收 + fencing=安全边界 =================
	fmt.Println("\n—— 第 4 章：僵尸容器回收（sweep）与 ERR_FENCED 安全边界 ——")
	zombie := dispatchOnce() // free 用户拿到容器
	demo.Check(zombie != nil, "free 用户开始会话：%s ⇔ %s", zombie.id, zombie.container)
	fmt.Println("  [session] 用户直接关页面：心跳停（不 complete、不 release）")
	demo.Await("租约到期 sweep 回收", 5*time.Second, func() bool {
		return demo.State(pool, client, zombie.container) == "READY" &&
			demo.Field(pool, client, zombie.container, "ver") == "3"
	})
	if api.IsRunning(zombie.container) {
		api.Kill(zombie.container) // sweep 触发的强杀（生产由回收器调 Docker API）
	}
	demo.Check(demo.Field(pool, client, zombie.container, "lease_resets") == "1",
		"僵尸回收：心跳停 %v → sweep 强制回收，容器回池可再分配（lease_resets=1）", sessionVis)
	hbErr := pool.Heartbeat(ctx, kiki.Task{ID: zombie.container, Owner: zombie.id, Ver: zombie.token}, sessionVis)
	demo.Check(errors.Is(hbErr, kiki.ErrFenced),
		"安全边界：僵尸会话复活续租 → ERR_FENCED（网关对每次 exec/代理请求校验 token，fencing 在此升级为访问控制）")

	// ================= 第 5 章：坏容器毒丸（sweep 路径）=================
	fmt.Println("\n—— 第 5 章：启动即崩的容器 → lease_resets 超限 → DLQ ——")
	again := dispatchOnce() // 坏容器被再次分配……
	demo.Check(again != nil && again.container == zombie.container, "坏容器再次被分配（它会再次崩溃）")
	fmt.Println("  [session] 该容器启动即崩，会话秒退，无人调 fail")
	demo.Await("sweep 路径死信", 5*time.Second, func() bool {
		es, _ := pool.ListDLQ(ctx, 10)
		return len(es) == 1
	})
	es, _ := pool.ListDLQ(ctx, 10)
	demo.Check(es[0].Via == "sweep" && es[0].Err == "lease_exceeded",
		"毒丸防线第二道：lease_resets=2 > MaxRedeliveries=1 → DLQ（err=lease_exceeded）——坏容器停止在「分配→崩溃→回收→再分配」里无限烧冷启动")

	fmt.Println(`
—— 场景演示结束：队列原语 ↔ 沙箱生命周期 ——
  enqueue 预热进池 / reserve=分配容器（原子互斥）/ heartbeat=会话续约 / complete=正常结束
  fail=供给失败退避 → DLQ / sweep+fencing=僵尸回收与安全边界 / lease_resets=坏容器毒丸
生产化差距（文档 §四，演示未覆盖）：max_hold 绝对持有上限；bind.lua 跨 hash tag 仅单机合法
（Cluster 需同 slot 对齐）；放置（bin-packing）不归队列管；单用户公平性限流。`)
}

func atoi(s string) int64 {
	var v int64
	for i := 0; i < len(s); i++ {
		v = v*10 + int64(s[i]-'0')
	}
	return v
}
