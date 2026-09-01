package kiki

import (
	"math/rand"
	"time"
)

// BackoffPolicy 决定 fail 后的退避时长。策略在客户端、机制在脚本
// （design.md §5.5 的分层）：SDK 算好 backoff_ms 传给 fail.lua，
// 未来换线性退避或按错误分类退避，不动热路径脚本。
type BackoffPolicy interface {
	// Next 入参 tries 为已尝试次数（从 1 起）。
	Next(tries int, rng *rand.Rand) time.Duration
}

// ExponentialBackoff 返回 min(cap, base·2^(tries-1)) · (1+U(−j, +j))。
// 抖动不可省：几千个任务同时失败时，无抖动的指数退避会形成同步重试风暴。
// 默认 base=1s cap=60s jitter=0.5。
func ExponentialBackoff(base, cap time.Duration, jitter float64) BackoffPolicy {
	if base <= 0 {
		base = time.Second
	}
	if cap < base {
		cap = base
	}
	if jitter < 0 {
		jitter = 0
	}
	return &expBackoff{base: base, cap: cap, jitter: jitter}
}

type expBackoff struct {
	base, cap time.Duration
	jitter    float64
}

func (b *expBackoff) Next(tries int, rng *rand.Rand) time.Duration {
	if tries < 1 {
		tries = 1
	}
	d := b.base
	for i := 1; i < tries; i++ {
		d *= 2
		if d >= b.cap {
			d = b.cap
			break
		}
	}
	if d > b.cap {
		d = b.cap
	}
	if b.jitter > 0 && rng != nil {
		f := 1 + (rng.Float64()*2-1)*b.jitter
		d = time.Duration(float64(d) * f)
	}
	if d < 0 {
		d = 0
	}
	return d
}

// fixedBackoff 恒定退避（测试与特殊场景用）。
type fixedBackoff time.Duration

func (f fixedBackoff) Next(int, *rand.Rand) time.Duration { return time.Duration(f) }

// FixedBackoff 返回恒定退避策略。
func FixedBackoff(d time.Duration) BackoffPolicy { return fixedBackoff(d) }
