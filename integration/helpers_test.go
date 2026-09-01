//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/garfield-dev-team/kiki"
)

// ---- 指标断言（每个 Queue 一个临时 Registerer，天然隔离）----

type registryStore struct {
	mu  sync.Mutex
	m   map[string]*prometheus.Registry
	seq int
}

var testRegistry = &registryStore{m: map[string]*prometheus.Registry{}}

func (s *registryStore) forQueue(name string) *prometheus.Registry {
	s.mu.Lock()
	defer s.mu.Unlock()
	reg := prometheus.NewRegistry()
	s.m[name] = reg
	return reg
}

func (s *registryStore) get(name string) *prometheus.Registry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[name]
}

// assertCounter 断言某 counter 的指定标签值。
func assertCounter(t *testing.T, queue, name, labelValue string, want int) {
	t.Helper()
	reg := testRegistry.get(queue)
	if reg == nil {
		t.Fatal("no registry for queue")
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := 0
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetValue() == labelValue {
					got += int(m.GetCounter().GetValue())
				}
			}
		}
	}
	if got != want {
		t.Fatalf("%s{%s=%q} = %d, want %d", name, "result", labelValue, got, want)
	}
}

// ---- T12 Cluster 子用例 ----

func clusterQueue(t *testing.T, c redis.UniversalClient) *kiki.Queue {
	t.Helper()
	// cluster 是跨进程共享的（compose 起一次跑多次），队列名带进程随机
	// 基址避免与上一轮失败的运行撞 key。
	name := fmt.Sprintf("cq%d", time.Now().UnixNano()%1_000_000+queueSeq.Add(1))
	q, err := kiki.NewQueue(kiki.QueueOptions{
		Redis:  c,
		Name:   name,
		Logger: discardLogger(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// clusterT1：T1 的集群版（hash tag 路由验证）。
func clusterT1(t *testing.T, c *redis.ClusterClient) {
	q := clusterQueue(t, c)
	ctx := context.Background()
	if err := q.Enqueue(ctx, q.Name()+"-ct1", []byte("body")); err != nil {
		t.Fatal(err)
	}
	nonEmpty := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ts, err := q.Reserve(ctx, 30*time.Second, 1)
			if err != nil {
				t.Errorf("reserve: %v", err)
				return
			}
			if len(ts) > 0 {
				mu.Lock()
				nonEmpty++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if nonEmpty != 1 {
		t.Fatalf("exactly one reserve must succeed, got %d", nonEmpty)
	}
}

// clusterT3：T3 的集群版（sweep 重投）。
func clusterT3(t *testing.T, c *redis.ClusterClient) {
	q := clusterQueue(t, c)
	ctx := context.Background()
	if err := q.Enqueue(ctx, q.Name()+"-ct3", []byte("body")); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Reserve(ctx, 200*time.Millisecond, 1); err != nil {
		t.Fatal(err)
	}
	s := q.NewScheduler(kiki.SchedulerOptions{Interval: 40 * time.Millisecond})
	sctx, cancel := context.WithCancel(ctx)
	go s.Run(sctx)
	defer cancel()
	var got []kiki.Task
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ts, err := q.Reserve(ctx, 30*time.Second, 1)
		if err == nil && len(ts) == 1 {
			got = ts
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// 集群冷启动的连接建立可能拖慢首投后的重领，轮次不与单机 T3 逐一对应；
	// 但 fencing 不变式必须成立：只有 sweep 重投（ver+1）与 reserve 颁发（ver+1）
	// 交替 ⇒ 第 k 次投递 ver = 2k-1。
	if len(got) != 1 || got[0].Tries < 2 || got[0].Ver != int64(2*got[0].Tries-1) {
		t.Fatalf("cluster redelivery (fencing invariant): %+v", got)
	}
}

// clusterT7：T7 的集群版（fail 路径毒丸）。
func clusterT7(t *testing.T, c *redis.ClusterClient) {
	q := clusterQueue(t, c)
	ctx := context.Background()
	if err := q.Enqueue(ctx, q.Name()+"-ct7", []byte("poison")); err != nil {
		t.Fatal(err)
	}
	s := q.NewScheduler(kiki.SchedulerOptions{Interval: 20 * time.Millisecond})
	sctx, cancel := context.WithCancel(ctx)
	go s.Run(sctx)
	defer cancel()
	var ts []kiki.Task
	// clusterQueue 无 MaxRetries 定制：队列默认 5 ⇒ 第 6 次 fail 才 DeadLettered。
	for round := 1; round <= 6; round++ {
		if round > 1 {
			waitForCond(t, 3*time.Second, func() bool {
				got, err := q.Reserve(ctx, time.Second, 1)
				if err != nil || len(got) == 0 {
					return false
				}
				ts = got
				return true
			})
		} else {
			var err error
			ts, err = q.Reserve(ctx, 30*time.Second, 1)
			if err != nil || len(ts) != 1 {
				t.Fatalf("round 1 reserve: %v %v", ts, err)
			}
		}
		res, ferr := q.Fail(ctx, ts[0], fmt.Errorf("boom-%d", round), time.Millisecond)
		if round < 6 && (ferr != nil || res != kiki.Retried) {
			t.Fatalf("round %d fail: %v %v", round, res, ferr)
		}
		if round == 6 && (ferr != nil || res != kiki.DeadLettered) {
			t.Fatalf("round 6 fail: %v %v", res, ferr)
		}
	}
	entries, err := q.ListDLQ(ctx, 10)
	if err != nil || len(entries) != 1 || entries[0].Via != "fail" {
		t.Fatalf("cluster dlq: %v %v", entries, err)
	}
}

// waitForCond 是 helpers 内的轮询等待。
func waitForCond(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

// ---- T13 子进程 ----

func childProcess(t *testing.T, exe, addr, qname, id string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(exe, "-test.run", "TestT13ChildHelper")
	cmd.Env = append(os.Environ(),
		"KIKI_CHILD=1",
		"KIKI_TEST_ADDR="+addr,
		"KIKI_TEST_QUEUE="+qname,
		"KIKI_TEST_TASK="+id,
	)
	return cmd
}

var _ = errors.New
