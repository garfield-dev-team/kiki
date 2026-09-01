// Package rdb 是 SDK 的 Redis 执行层：UniversalClient 构造、key 布局、
// 脚本加载与 warmup。一切状态转移经由此层的嵌入式脚本执行，Go 侧不存在
// 任何内联 eval 字符串（go-implementation.md §3）。
package rdb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Keys 是一个队列的全部 key。所有条目共享 hash tag {qk:<name>}，保证同队列
// 落在同一 cluster slot（Cluster 前提）。task key 前缀以 ':' 结尾，脚本经
// ARGV 动态拼接 task id。
type Keys struct {
	Ready      string // ZSET member=task_id, score=pri×2⁴⁸+ts_ms
	Sched      string // ZSET member=task_id, score=visible_at_ms
	Lease      string // ZSET member=task_id, score=lease_deadline_ms
	DLQ        string // Stream 死信快照
	TaskPrefix string // task hash 前缀 {qk:q}:t:
}

// BuildKeys 按队列名构造 key 布局（代码即文档，design.md §4.3）。
func BuildKeys(name string) Keys {
	p := "{qk:" + name + "}"
	return Keys{
		Ready:      p + ":ready",
		Sched:      p + ":sched",
		Lease:      p + ":lease",
		DLQ:        p + ":dlq",
		TaskPrefix: p + ":t:",
	}
}

// DedupKey 是消费侧幂等键。刻意不带 hash tag：它不属于任何队列的状态机，
// cluster 自由分布即可（go-implementation.md §3.3）。
func DedupKey(taskID string) string { return "kiki:dedup:" + taskID }

// Engine 持有一个队列的全部脚本与 key 布局。
type Engine struct {
	client      redis.UniversalClient
	keys        Keys
	scripts     map[string]*redis.Script
	readTimeout time.Duration
}

// New 构造引擎并从 src 加载 scripts/*.lua。
func New(client redis.UniversalClient, name string, src fs.FS, readTimeout time.Duration) (*Engine, error) {
	if client == nil {
		return nil, errors.New("kiki: nil redis client")
	}
	e := &Engine{
		client:      client,
		keys:        BuildKeys(name),
		scripts:     make(map[string]*redis.Script),
		readTimeout: readTimeout,
	}
	matches, err := fs.Glob(src, "scripts/*.lua")
	if err != nil {
		return nil, fmt.Errorf("kiki: glob scripts: %w", err)
	}
	if len(matches) == 0 {
		return nil, errors.New("kiki: no scripts embedded")
	}
	for _, path := range matches {
		s, err := fs.ReadFile(src, path)
		if err != nil {
			return nil, fmt.Errorf("kiki: read %s: %w", path, err)
		}
		base := path[strings.LastIndex(path, "/")+1:]
		e.scripts[strings.TrimSuffix(base, ".lua")] = redis.NewScript(string(s))
	}
	return e, nil
}

// Client 暴露底层连接供 Queue 的只读运维命令（ZCARD/XRANGE/TIME）使用。
// 写路径必须走脚本，这是纪律不是建议。
func (e *Engine) Client() redis.UniversalClient { return e.client }

// Keys 返回本队列 key 布局。
func (e *Engine) Keys() Keys { return e.keys }

// Warmup 对全部脚本做 SCRIPT LOAD（集群每 master 一次），把首次调用可能的
// NOSCRIPT 回退抖动挪到启动期。失败非致命——Script.Run 自带
// EVALSHA→NOSCRIPT→EVAL 回退。
func (e *Engine) Warmup(ctx context.Context) error {
	load := func(c redis.Cmdable) error {
		for _, s := range e.scripts {
			if err := s.Load(ctx, c).Err(); err != nil {
				return err
			}
		}
		return nil
	}
	switch c := e.client.(type) {
	case *redis.ClusterClient:
		return c.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
			return load(client)
		})
	default:
		return load(e.client)
	}
}

// Run 执行一个脚本并返回原始 reply（Lua 表 → []any，字符串元素）。
func (e *Engine) Run(ctx context.Context, name string, keys []string, args ...any) ([]any, error) {
	s := e.scripts[name]
	if s == nil {
		return nil, fmt.Errorf("kiki: unknown script %q", name)
	}
	opCtx, cancel := context.WithTimeout(ctx, e.readTimeout)
	defer cancel()
	v, err := s.Run(opCtx, e.client, keys, args...).Result()
	if err != nil {
		return nil, err
	}
	list, ok := v.([]any)
	if !ok && v != nil {
		return nil, fmt.Errorf("kiki: script %q: unexpected reply type %T", name, v)
	}
	return list, nil
}

// Call 是一次流水线脚本调用。
type Call struct {
	Name string
	Keys []string
	Args []any
}

// PipelineRun 以单次 RTT 执行 N 个脚本调用，返回每个调用的原始 reply。
// 任一命中 NOSCRIPT（脚本缓存被 flush）时返回 ErrNoScript，调用方应回退为
// 逐个 Run（自带 EVAL 回退）。
func (e *Engine) PipelineRun(ctx context.Context, calls []Call) ([][]any, error) {
	if len(calls) == 0 {
		return nil, nil
	}
	opCtx, cancel := context.WithTimeout(ctx, e.readTimeout)
	defer cancel()
	pipe := e.client.Pipeline()
	cmds := make([]*redis.Cmd, len(calls))
	for i, call := range calls {
		s := e.scripts[call.Name]
		if s == nil {
			return nil, fmt.Errorf("kiki: unknown script %q", call.Name)
		}
		cmds[i] = s.EvalSha(opCtx, pipe, call.Keys, call.Args...)
	}
	if _, err := pipe.Exec(opCtx); err != nil {
		// Exec 返回首个错误；逐条检查后决定是否可回退。
	}
	out := make([][]any, len(calls))
	for i, cmd := range cmds {
		v, err := cmd.Result()
		if err != nil {
			if isNoScript(err) {
				return nil, ErrNoScript
			}
			return nil, err
		}
		list, _ := v.([]any)
		out[i] = list
	}
	return out, nil
}

// ErrNoScript 表示脚本缓存缺失，需要走逐条 Run 的 EVAL 回退。
var ErrNoScript = errors.New("kiki: NOSCRIPT (script cache flushed)")

func isNoScript(err error) bool {
	if err == nil {
		return false
	}
	return strings.HasPrefix(strings.ToUpper(err.Error()), "NOSCRIPT")
}

// AsString 断言 reply 元素为字符串。
func AsString(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("kiki: unexpected reply element %T", v)
	}
	return s, nil
}

// AsList 断言 reply 元素为嵌套表。
func AsList(v any) ([]any, error) {
	l, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("kiki: unexpected reply row %T", v)
	}
	return l, nil
}
