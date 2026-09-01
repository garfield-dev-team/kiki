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
  stats <queue>                     深度与 oldest_ready_age
  inspect <queue> <task_id>         任务全字段（forensic）
  enqueue <queue> --id ID [--pri N] [--delay DUR] [--body S | --body @file]
  dlq ls <queue> [--count N]        浏览死信快照
  dlq replay <queue> [--filter k=v] [--count N] [--force] [--dry-run]
  sweep <queue> [--limit N]         手动触发一轮 sweep+promote（救火模式）
  version                           汇报库/脚本版本
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

func cmdStats(ctx context.Context, client redis.UniversalClient, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: kikictl stats <queue>")
	}
	q, err := openQueue(client, args[0])
	if err != nil {
		return err
	}
	defer q.Close()
	st, err := q.Stats(ctx)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ready\tsched\tlease\tdlq\toldest_ready_age")
	fmt.Fprintf(w, "%d\t%d\t%d\t%d\t%s\n", st.ReadyDepth, st.SchedDepth, st.LeaseDepth, st.DLQLen, st.OldestReadyAge.Truncate(time.Millisecond))
	return w.Flush()
}

func cmdInspect(ctx context.Context, client redis.UniversalClient, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: kikictl inspect <queue> <task_id>")
	}
	q, err := openQueue(client, args[0])
	if err != nil {
		return err
	}
	defer q.Close()
	vals, err := q.Client().HGetAll(ctx, fmt.Sprintf("{qk:%s}:t:%s", args[0], args[1])).Result()
	if err != nil {
		return err
	}
	if len(vals) == 0 {
		return fmt.Errorf("task %s not found (retention expired?)", args[1])
	}
	keys := make([]string, 0, len(vals))
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("%-14s %s\n", k, vals[k])
	}
	return nil
}

func cmdEnqueue(ctx context.Context, client redis.UniversalClient, args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	id := fs.String("id", "", "任务 id（业务唯一键）")
	pri := fs.Int("pri", 0, "优先级（0 最高）")
	delay := fs.Duration("delay", 0, "延迟投递（如 30s）")
	body := fs.String("body", "", "payload 字符串，或 @file 从文件读取")
	maxRetries := fs.Int("max-retries", 0, "重试上限（0 = 队列默认）")
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
	q, err := openQueue(client, queue)
	if err != nil {
		return err
	}
	defer q.Close()
	var opts []kiki.EnqueueOption
	opts = append(opts, kiki.WithPriority(*pri))
	if *maxRetries > 0 {
		opts = append(opts, kiki.WithMaxRetries(*maxRetries))
	}
	if *delay > 0 {
		return q.EnqueueIn(ctx, *id, payload, *delay, opts...)
	}
	return q.Enqueue(ctx, *id, payload, opts...)
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
		q, err := openQueue(client, queue)
		if err != nil {
			return err
		}
		defer q.Close()
		entries, err := q.ListDLQ(ctx, *count)
		if err != nil {
			return err
		}
		w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
		fmt.Fprintln(w, "stream_id\ttask_id\tvia\tries\tts\terr")
		for _, e := range entries {
			fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%s\n",
				e.StreamID, e.ID, e.Via, e.Tries, e.TS.Format(time.RFC3339), firstLine(e.Err))
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
		q, err := openQueue(client, queue)
		if err != nil {
			return err
		}
		defer q.Close()
		entries, err := q.ListDLQ(ctx, *count)
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
				fmt.Printf("  - %s via=%s tries=%d err=%s\n", e.ID, e.Via, e.Tries, firstLine(e.Err))
			}
			return nil
		}
		if err := q.ReplayDLQ(ctx, selected, *force); err != nil {
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
	q, err := openQueue(client, queue)
	if err != nil {
		return err
	}
	defer q.Close()
	st, err := q.SweepOnce(ctx, *limit)
	if err != nil {
		return err
	}
	fmt.Printf("requeued=%d dlq=%d\n", st.Requeued, st.DeadLettered)
	return nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

var _ = json.Marshal // 保留：dump 扩展位
