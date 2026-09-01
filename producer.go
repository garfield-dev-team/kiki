package kiki

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/garfield-dev-team/kiki/internal/rdb"
)

// Enqueue 立即投递（转移 #1）。id 已存在时返回 ErrDup——这是生产者重试的
// 幂等防线，不是错误情形。
func (q *Queue) Enqueue(ctx context.Context, id string, payload []byte, opts ...EnqueueOption) error {
	t := Task{ID: id, Payload: payload}
	for _, o := range opts {
		o(&t)
	}
	return q.enqueueTask(ctx, t)
}

// EnqueueIn 延迟投递（转移 #2）。
func (q *Queue) EnqueueIn(ctx context.Context, id string, payload []byte, delay time.Duration, opts ...EnqueueOption) error {
	t := Task{ID: id, Payload: payload}
	for _, o := range opts {
		o(&t)
	}
	t.Delay = delay
	return q.enqueueTask(ctx, t)
}

// EnqueueBulk 以单次流水线 RTT 批量投递。任一校验失败则整批拒绝（不产生
// 半批）；脚本级失败（如 ErrDup）逐条报告。
func (q *Queue) EnqueueBulk(ctx context.Context, tasks []Task) error {
	if err := q.checkOpen(); err != nil {
		return err
	}
	calls := make([]rdb.Call, len(tasks))
	var errs []error
	for i, t := range tasks {
		if err := q.validateTask(&t); err != nil {
			return fmt.Errorf("kiki: task %d (%s): %w", i, t.ID, err)
		}
		hdr, err := encodeHeaders(t.Headers)
		if err != nil {
			return fmt.Errorf("kiki: task %d (%s): %w", i, t.ID, err)
		}
		calls[i] = q.enqueueCall(t, hdr)
	}
	reply, err := q.eng.PipelineRun(ctx, calls)
	if errors.Is(err, rdb.ErrNoScript) {
		// 脚本缓存被 flush：回退为逐条 Run（自带 EVALSHA→EVAL 回退）。
		for i := range calls {
			st, fields, err := q.call(ctx, calls[i].Name, calls[i].Keys, calls[i].Args...)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			errs = append(errs, q.enqueueResult(tasks[i], st, fields))
		}
		return errors.Join(errs...)
	}
	if err != nil {
		return err
	}
	for i, row := range reply {
		var st string
		var fields []string
		if len(row) == 0 {
			st, fields = "", nil
			errs = append(errs, fmt.Errorf("kiki: enqueue %s: empty reply", tasks[i].ID))
			q.metr.EnqueueTotal("err")
			continue
		}
		st, err = rdb.AsString(row[0])
		if err != nil {
			errs = append(errs, err)
			q.metr.EnqueueTotal("err")
			continue
		}
		for _, f := range row[1:] {
			fields = append(fields, fmt.Sprint(f))
		}
		errs = append(errs, q.enqueueResult(tasks[i], st, fields))
	}
	return errors.Join(errs...)
}

func (q *Queue) enqueueTask(ctx context.Context, t Task) error {
	if err := q.checkOpen(); err != nil {
		return err
	}
	if err := q.validateTask(&t); err != nil {
		return err
	}
	hdr, err := encodeHeaders(t.Headers)
	if err != nil {
		return err
	}
	call := q.enqueueCall(t, hdr)
	st, fields, err := q.call(ctx, call.Name, call.Keys, call.Args...)
	if err != nil {
		q.metr.EnqueueTotal("err")
		return err
	}
	return q.enqueueResult(t, st, fields)
}

func (q *Queue) enqueueResult(t Task, status string, fields []string) error {
	switch status {
	case "OK":
		q.metr.EnqueueTotal("ok")
		return nil
	case "ERR_DUP":
		q.metr.EnqueueTotal("dup")
		return statusError(status, fields)
	default:
		q.metr.EnqueueTotal("err")
		return statusError(status, fields)
	}
}

func (q *Queue) enqueueCall(t Task, headersJSON string) rdb.Call {
	k := q.eng.Keys()
	delayMs := t.Delay.Milliseconds()
	if delayMs < 0 {
		delayMs = 0
	}
	return rdb.Call{
		Name: "enqueue",
		Keys: []string{k.TaskPrefix + t.ID, k.Ready, k.Sched},
		Args: []any{t.ID, string(t.Payload), t.Priority, delayMs, q.resolveMaxRetries(t), headersJSON},
	}
}

func (q *Queue) validateTask(t *Task) error {
	if err := validateID(t.ID); err != nil {
		return err
	}
	if int64(len(t.Payload)) > q.payloadLimit {
		return fmt.Errorf("%w: %d > %d bytes", ErrPayloadTooLarge, len(t.Payload), q.payloadLimit)
	}
	if t.Priority < 0 {
		return fmt.Errorf("%w: negative priority breaks ready-score encoding", ErrInvalidArgument)
	}
	if t.MaxRetries < 0 {
		return fmt.Errorf("%w: negative MaxRetries", ErrInvalidArgument)
	}
	return nil
}

func (q *Queue) resolveMaxRetries(t Task) int {
	if t.MaxRetries == 0 {
		return q.maxRetries // 0 → 队列默认（§2.1）
	}
	return t.MaxRetries
}

func encodeHeaders(h map[string]string) (string, error) {
	if len(h) == 0 {
		return "", nil
	}
	b, err := json.Marshal(h)
	if err != nil {
		return "", fmt.Errorf("encode headers: %w", err)
	}
	return string(b), nil
}
