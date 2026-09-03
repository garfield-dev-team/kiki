// kikictl 是 kiki 的运维 CLI（go-implementation.md §11），复用 Queue 公共 API。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/garfield-dev-team/kiki"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "kikictl:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("kikictl", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:6379", "Redis 地址（单机/Sentinel）")
	addrs := fs.String("addrs", "", "Redis 集群地址列表（逗号分隔，设置后忽略 -addr）")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `kikictl — kiki 任务队列运维工具

用法: kikictl [-addr host:port | -addrs a,b,c] <命令> [参数]

命令:
  stats <queue>                     深度与 oldest_ready_age（分片队列自动聚合并列出各分片）
  inspect <queue> <task_id>         任务全字段（forensic；分片队列用 --shard 指定，缺省逐分片探测）
  enqueue <queue> --id ID [--pri N] [--delay DUR] [--body S | --body @file] [--route-key K]
  dlq ls <queue> [--count N]        浏览死信快照（分片队列自动合并，附 shard 列）
  dlq replay <queue> [--filter k=v] [--count N] [--force] [--dry-run]
  sweep <queue> [--limit N]         手动触发一轮 sweep+promote（救火模式；分片队列扇出全分片）
  sq manifest get <queue>           查看分片队列 manifest（N 的事实源）
  sq manifest set <queue> --shards N [--force]
                                    变更 N：只允许单调扩；缩 N 需被移除分片全空或 --force
  version                           汇报库/脚本版本

stats/enqueue/dlq/sweep 对分片队列自动感知（manifest 探测），单队列行为不变。
`)
		fs.PrintDefaults()
	}
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		fs.Usage()
		return fmt.Errorf("missing command")
	}

	var opts redis.UniversalOptions
	if *addrs != "" {
		opts.Addrs = strings.Split(*addrs, ",")
	} else {
		opts.Addrs = []string{*addr}
	}
	rdbClient := redis.NewUniversalClient(&opts)
	defer rdbClient.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cmd, args := fs.Arg(0), fs.Args()[1:]
	switch cmd {
	case "version":
		q, err := openQueue(rdbClient, "ctl")
		if err != nil {
			return err
		}
		_ = q.Close()
		fmt.Println(q.Version())
		return nil
	case "stats":
		return cmdStats(ctx, rdbClient, args)
	case "inspect":
		return cmdInspect(ctx, rdbClient, args)
	case "enqueue":
		return cmdEnqueue(ctx, rdbClient, args)
	case "dlq":
		return cmdDLQ(ctx, rdbClient, args)
	case "sweep":
		return cmdSweep(ctx, rdbClient, args)
	case "sq":
		return cmdSQ(ctx, rdbClient, args)
	default:
		fs.Usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// pullQueue 从子命令参数中摘出队列名（第一个非 flag 参数），
// 让 `enqueue <queue> --id ...` 这种 flag 跟在位置参数后的写法可用。
func pullQueue(args []string) (queue string, rest []string, err error) {
	for i, a := range args {
		if !strings.HasPrefix(a, "-") {
			rest := append(append([]string{}, args[:i]...), args[i+1:]...)
			return a, rest, nil
		}
		if a == "--" {
			break
		}
	}
	return "", args, fmt.Errorf("missing <queue> argument")
}

func openQueue(client redis.UniversalClient, name string) (*kiki.Queue, error) {
	q, err := kiki.NewQueue(kiki.QueueOptions{Redis: client, Name: name})
	if err != nil {
		return nil, err
	}
	return q, nil
}

// ---- 分片感知的队列句柄：manifest 存在 ⇒ ShardedQueue，否则单 Queue ----

type ctlQueue struct {
	q  *kiki.Queue
	sq *kiki.ShardedQueue
}

// openAny 按逻辑队列名打开队列，manifest 探测对调用方透明。
func openAny(ctx context.Context, client redis.UniversalClient, name string) (*ctlQueue, error) {
	n, ok, err := kiki.LookupManifest(ctx, client, name)
	if err != nil {
		return nil, err
	}
	if ok {
		sq, err := kiki.NewShardedQueue(kiki.ShardedOptions{Redis: client, Name: name, Shards: n})
		if err != nil {
			return nil, err
		}
		return &ctlQueue{sq: sq}, nil
	}
	q, err := openQueue(client, name)
	if err != nil {
		return nil, err
	}
	return &ctlQueue{q: q}, nil
}

func (c *ctlQueue) Close() {
	if c.sq != nil {
		_ = c.sq.Close()
		return
	}
	_ = c.q.Close()
}

func (c *ctlQueue) stats(ctx context.Context) (kiki.ShardedStats, error) {
	if c.sq != nil {
		return c.sq.Stats(ctx)
	}
	st, err := c.q.Stats(ctx)
	return kiki.ShardedStats{Stats: st, Shards: []kiki.Stats{st}}, err
}

func (c *ctlQueue) listDLQ(ctx context.Context, count int64) ([]kiki.DLQEntry, error) {
	if c.sq != nil {
		return c.sq.ListDLQ(ctx, count)
	}
	return c.q.ListDLQ(ctx, count)
}

func (c *ctlQueue) replayDLQ(ctx context.Context, entries []kiki.DLQEntry, force bool) error {
	if c.sq != nil {
		return c.sq.ReplayDLQ(ctx, entries, force)
	}
	return c.q.ReplayDLQ(ctx, entries, force)
}

func (c *ctlQueue) sweepOnce(ctx context.Context, limit int) (kiki.SweepStats, error) {
	if c.sq != nil {
		return c.sq.SweepOnce(ctx, limit)
	}
	return c.q.SweepOnce(ctx, limit)
}

func (c *ctlQueue) enqueue(ctx context.Context, id string, payload []byte, delay time.Duration, opts ...kiki.EnqueueOption) error {
	if c.sq != nil {
		if delay > 0 {
			return c.sq.EnqueueIn(ctx, id, payload, delay, opts...)
		}
		return c.sq.Enqueue(ctx, id, payload, opts...)
	}
	if delay > 0 {
		return c.q.EnqueueIn(ctx, id, payload, delay, opts...)
	}
	return c.q.Enqueue(ctx, id, payload, opts...)
}

// taskKey 返回 task hash 的 key；分片队列需指定分片（inspect 用）。
func (c *ctlQueue) taskKey(shard int, id string) string {
	name := c.name()
	if c.sq != nil {
		name = fmt.Sprintf("%s#%d", name, shard)
	}
	return fmt.Sprintf("{qk:%s}:t:%s", name, id)
}

func (c *ctlQueue) name() string {
	if c.sq != nil {
		return c.sq.Name()
	}
	return c.q.Name()
}

func (c *ctlQueue) sharded() bool { return c.sq != nil }

func cmdStats(ctx context.Context, client redis.UniversalClient, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: kikictl stats <queue>")
	}
	cq, err := openAny(ctx, client, args[0])
	if err != nil {
		return err
	}
	defer cq.Close()
	st, err := cq.stats(ctx)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "queue\tready\tsched\tlease\tdlq\toldest_ready_age")
	row := func(name string, s kiki.Stats) {
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%s\n", name, s.ReadyDepth, s.SchedDepth, s.LeaseDepth, s.DLQLen, s.OldestReadyAge.Truncate(time.Millisecond))
	}
	row(cq.name(), st.Stats)
	if cq.sharded() {
		for k, s := range st.Shards {
			row(fmt.Sprintf("%s#%d", cq.name(), k), s)
		}
	}
	return w.Flush()
}

func cmdInspect(ctx context.Context, client redis.UniversalClient, args []string) error {
	fs := flag.NewFlagSet("inspect", flag.ExitOnError)
	shard := fs.Int("shard", -1, "分片号（分片队列缺省逐分片探测）")
	queue, rest, err := pullQueue(args)
	if err != nil {
		return err
	}
	if err := fs.Parse(rest); err != nil {
		return err
	}
	rest = fs.Args()
	if len(rest) != 1 {
		return fmt.Errorf("usage: kikictl inspect <queue> <task_id>")
	}
	id := rest[0]
	cq, err := openAny(ctx, client, queue)
	if err != nil {
		return err
	}
	defer cq.Close()
	var keys []string
	switch {
	case !cq.sharded():
		keys = []string{cq.taskKey(0, id)}
	case *shard >= 0:
		keys = []string{cq.taskKey(*shard, id)}
	default:
		for k := 0; k < cq.sq.Shards(); k++ {
			keys = append(keys, cq.taskKey(k, id))
		}
	}
	for _, key := range keys {
		vals, err := client.HGetAll(ctx, key).Result()
		if err != nil {
			return err
		}
		if len(vals) == 0 {
			continue
		}
		fmt.Printf("%s\n", key)
		ordered := make([]string, 0, len(vals))
		for k := range vals {
			ordered = append(ordered, k)
		}
		sort.Strings(ordered)
		for _, k := range ordered {
			fmt.Printf("%-14s %s\n", k, vals[k])
		}
		return nil
	}
	return fmt.Errorf("task %s not found (retention expired? wrong --shard?)", id)
}

func cmdEnqueue(ctx context.Context, client redis.UniversalClient, args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	id := fs.String("id", "", "任务 id（业务唯一键）")
	pri := fs.Int("pri", 0, "优先级（0 最高）")
	delay := fs.Duration("delay", 0, "延迟投递（如 30s）")
	body := fs.String("body", "", "payload 字符串，或 @file 从文件读取")
	maxRetries := fs.Int("max-retries", 0, "重试上限（0 = 队列默认）")
	routeKey := fs.String("route-key", "", "分片路由键（仅分片队列感知；同键恒落同分片）")
	queue, rest, err := pullQueue(args)
	if err != nil {
		return err
	}
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if *id == "" {
		return fmt.Errorf("usage: kikictl enqueue <queue> --id ID [--body ...]")
	}
	payload := []byte(*body)
	if strings.HasPrefix(*body, "@") {
		b, err := os.ReadFile(strings.TrimPrefix(*body, "@"))
		if err != nil {
			return err
		}
		payload = b
	}
	cq, err := openAny(ctx, client, queue)
	if err != nil {
		return err
	}
	defer cq.Close()
	var opts []kiki.EnqueueOption
	opts = append(opts, kiki.WithPriority(*pri))
	if *maxRetries > 0 {
		opts = append(opts, kiki.WithMaxRetries(*maxRetries))
	}
	if *routeKey != "" {
		opts = append(opts, kiki.WithRouteKey(*routeKey))
	}
	return cq.enqueue(ctx, *id, payload, *delay, opts...)
}

func cmdDLQ(ctx context.Context, client redis.UniversalClient, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: kikictl dlq <ls|replay> <queue>")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "ls":
		fs := flag.NewFlagSet("dlq ls", flag.ExitOnError)
		count := fs.Int64("count", 50, "最多显示条数")
		queue, rest, err := pullQueue(rest)
		if err != nil {
			return err
		}
		if err := fs.Parse(rest); err != nil {
			return err
		}
		cq, err := openAny(ctx, client, queue)
		if err != nil {
			return err
		}
		defer cq.Close()
		entries, err := cq.listDLQ(ctx, *count)
		if err != nil {
			return err
		}
		w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
		fmt.Fprintln(w, "stream_id\ttask_id\tshard\tvia\tries\tts\terr")
		for _, e := range entries {
			fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%d\t%s\t%s\n",
				e.StreamID, e.ID, e.Shard, e.Via, e.Tries, e.TS.Format(time.RFC3339), firstLine(e.Err))
		}
		return w.Flush()
	case "replay":
		fs := flag.NewFlagSet("dlq replay", flag.ExitOnError)
		filter := fs.String("filter", "", "快照字段过滤 k=v（可多次出现前仅取最后一对）")
		count := fs.Int64("count", 100, "单轮最多扫描条数")
		force := fs.Bool("force", false, "保留期内残留 hash 需显式授权（仅 DLQ 态可清）")
		dryRun := fs.Bool("dry-run", false, "只打印计划，不执行")
		queue, rest2, err := pullQueue(rest)
		if err != nil {
			return err
		}
		if err := fs.Parse(rest2); err != nil {
			return err
		}
		cq, err := openAny(ctx, client, queue)
		if err != nil {
			return err
		}
		defer cq.Close()
		entries, err := cq.listDLQ(ctx, *count)
		if err != nil {
			return err
		}
		var selected []kiki.DLQEntry
		if *filter != "" {
			k, v, ok := strings.Cut(*filter, "=")
			if !ok {
				return fmt.Errorf("--filter 需要 k=v 形式")
			}
			for _, e := range entries {
				if dlqField(e, k) == v {
					selected = append(selected, e)
				}
			}
		} else {
			selected = entries
		}
		if *dryRun {
			fmt.Printf("dry-run: 将重放 %d 条（force=%v）\n", len(selected), *force)
			for _, e := range selected {
				fmt.Printf("  - %s shard=%d via=%s tries=%d err=%s\n", e.ID, e.Shard, e.Via, e.Tries, firstLine(e.Err))
			}
			return nil
		}
		if err := cq.replayDLQ(ctx, selected, *force); err != nil {
			return err
		}
		fmt.Printf("已重放 %d 条（新 tries 周期）\n", len(selected))
		return nil
	default:
		return fmt.Errorf("unknown dlq subcommand %q", sub)
	}
}

func dlqField(e kiki.DLQEntry, k string) string {
	switch k {
	case "id":
		return e.ID
	case "via":
		return e.Via
	case "err":
		return e.Err
	case "tries":
		return strconv.Itoa(e.Tries)
	}
	return ""
}

func cmdSweep(ctx context.Context, client redis.UniversalClient, args []string) error {
	fs := flag.NewFlagSet("sweep", flag.ExitOnError)
	limit := fs.Int("limit", 200, "单轮上限")
	queue, rest, err := pullQueue(args)
	if err != nil {
		return err
	}
	if err := fs.Parse(rest); err != nil {
		return err
	}
	cq, err := openAny(ctx, client, queue)
	if err != nil {
		return err
	}
	defer cq.Close()
	st, err := cq.sweepOnce(ctx, *limit)
	if err != nil {
		return err
	}
	fmt.Printf("requeued=%d dlq=%d\n", st.Requeued, st.DeadLettered)
	return nil
}

// ---- sq：分片队列治理命令 ----

func cmdSQ(ctx context.Context, client redis.UniversalClient, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: kikictl sq <manifest> ...")
	}
	switch args[0] {
	case "manifest":
		return cmdManifest(ctx, client, args[1:])
	default:
		return fmt.Errorf("unknown sq subcommand %q", args[0])
	}
}

func cmdManifest(ctx context.Context, client redis.UniversalClient, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: kikictl sq manifest <get|set> <queue>")
	}
	switch args[0] {
	case "get":
		queue, _, err := pullQueue(args[1:])
		if err != nil {
			return err
		}
		n, ok, err := kiki.LookupManifest(ctx, client, queue)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("no manifest for %q (single queue or sharding not initialized)", queue)
		}
		fmt.Printf("%s shards=%d\n", queue, n)
		return nil
	case "set":
		fs := flag.NewFlagSet("sq manifest set", flag.ExitOnError)
		shards := fs.Int("shards", 0, "目标分片数 N")
		force := fs.Bool("force", false, "缩 N 时跳过'被移除分片非空'守卫（产生孤儿任务）")
		queue, rest, err := pullQueue(args[1:])
		if err != nil {
			return err
		}
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *shards == 0 {
			return fmt.Errorf("usage: kikictl sq manifest set <queue> --shards N")
		}
		cur, ok, err := kiki.LookupManifest(ctx, client, queue)
		if err != nil {
			return err
		}
		if err := kiki.SetManifest(ctx, client, queue, *shards, *force); err != nil {
			return err
		}
		if ok && *shards > cur {
			fmt.Printf("manifest %s: %d → %d（扩容）。后续：滚动重启生产者，再滚动重启消费者（旧 N 进程会被 Strict 校验拒绝启动），观察各分片深度。\n", queue, cur, *shards)
		} else if ok && *shards < cur {
			fmt.Printf("manifest %s: %d → %d（缩容）。被移除分片 %s#%d..%d 的存量任务已成孤儿（如为 force）；如未 force 则已排空。\n", queue, cur, *shards, queue, *shards, cur-1)
		} else {
			fmt.Printf("manifest %s: shards=%d\n", queue, *shards)
		}
		return nil
	default:
		return fmt.Errorf("unknown manifest subcommand %q", args[0])
	}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

var _ = json.Marshal // 保留：dump 扩展位
