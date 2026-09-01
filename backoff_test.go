package kiki

import (
	"math/rand"
	"testing"
	"time"
)

func TestExponentialBackoffShape(t *testing.T) {
	b := ExponentialBackoff(time.Second, 60*time.Second, 0) // 无抖动，断言确定性
	cases := []struct {
		tries int
		want  time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{7, 60 * time.Second},   // 64s 封顶
		{100, 60 * time.Second}, // 封顶后不发散
	}
	for _, c := range cases {
		if got := b.Next(c.tries, nil); got != c.want {
			t.Errorf("tries=%d: got %v want %v", c.tries, got, c.want)
		}
	}
}

func TestExponentialBackoffJitterBound(t *testing.T) {
	b := ExponentialBackoff(time.Second, 60*time.Second, 0.5)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 1000; i++ {
		d := b.Next(3, rng) // 基准 4s，抖动 ±50% ⇒ [2s, 6s]
		if d < 2*time.Second || d > 6*time.Second {
			t.Fatalf("jitter out of bounds: %v", d)
		}
	}
}

func TestErrWrappers(t *testing.T) {
	base := errTextWrap("boom")
	if PermanentOf(base) {
		t.Fatal("base should not be permanent")
	}
	p := NonRetryable(base)
	if !PermanentOf(p) || !PermanentOf(wrap(p)) {
		t.Fatal("NonRetryable mark should survive wrapping")
	}
	if _, ok := backoffOf(base); ok {
		t.Fatal("base should not carry backoff")
	}
	b := WithBackoff(base, 3*time.Second)
	d, ok := backoffOf(b)
	if !ok || d != 3*time.Second {
		t.Fatalf("backoff mark lost: %v %v", d, ok)
	}
	if NonRetryable(nil) != nil || WithBackoff(nil, time.Second) != nil {
		t.Fatal("nil passthrough broken")
	}
}

type errTextWrap string

func (e errTextWrap) Error() string { return string(e) }

type wrapErr struct{ inner error }

func (w wrapErr) Error() string { return "wrapped: " + w.inner.Error() }
func (w wrapErr) Unwrap() error { return w.inner }

func wrap(e error) error { return wrapErr{inner: e} }

// TestIDValidation 覆盖 key 注入防线（§2.5）。
func TestIDValidation(t *testing.T) {
	valid := []string{"a", "email:123:welcome", "A-b_c.9", string(make([]byte, 0)) + "x"}
	invalid := []string{"", " lead", "lead space", "sl/ash", "negative\x00", "{"}
	for _, id := range valid {
		if err := validateID(id); err != nil {
			t.Errorf("id %q should pass: %v", id, err)
		}
	}
	for _, id := range invalid {
		if err := validateID(id); err == nil {
			t.Errorf("id %q should fail", id)
		}
	}
	if err := validateQueueName("orders#0"); err != nil {
		t.Errorf("sharded name should pass: %v", err)
	}
	if err := validateQueueName("{evil}"); err == nil {
		t.Error("hash-tag-breaking name must fail")
	}
}
