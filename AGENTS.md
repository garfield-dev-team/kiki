# AGENTS.md — kiki 开发行为准则

本文件约束所有在本仓库工作的 agent（及人类贡献者）。它是行为准则，不是设计文档——设计看 `design.md` 与 `go-implementation.md`。

## 0. 项目速览

- **一句话**：基于 Redis 的生产级任务队列（中间件库 + Go Client SDK），状态机全部由 Lua 脚本原子实现，语义为 at-least-once + 消费侧幂等收敛。
- **技术栈**：Go 1.22+ / `github.com/redis/go-redis/v9` / `log/slog`；可选 `prometheus/client_golang`。零框架。
- **必读文档**（动手前按序读完，改动前重读相关章节）：
  0. `README.md` —— 项目概览、命名由来与文档地图（入口）
  1. `design.md` —— 状态机、脚本、竞态消灭矩阵（规范源头）
  2. `go-implementation.md` —— Go 工程结构、并发模型、黄金用例 T1–T13
  3. `scripts/*.lua` —— 脚本规范版（勘误记录见 go-implementation.md §3.5，勘误 append-only，不得删改）
- **真相裁决顺序**：`scripts/` + 集成测试 > `design.md` > `go-implementation.md` > 本文件。文档与代码冲突时先修文档，不得让代码迁就过时文档后蒙混过关。

## 1. 环境与常用命令

```bash
go build ./...                          # 编译
go test ./...                           # 单测（miniredis，仅纯 Go 逻辑）
go test -race ./...                     # 竞态门禁，必过
go test -tags=integration ./integration/  # 黄金用例 T1–T13（需 Docker，testcontainers 拉起真实 redis:7.2）
go test -race -count=10 ./integration/    # flaky 检查（脚本改动后必跑）
go test -bench=. -run=^$ ./...          # 基准（对照 go-implementation.md §10.2 基线）
docker compose -f docker-compose.test.yml up -d   # 本地单机 + 4-master cluster
golangci-lint run                       # lint
```

**环境受限时的诚实条款**：无 Docker 则集成测试不可运行。此时在交付说明中明确写"集成未验证"，禁止声称脚本改动已验证通过。

## 2. 硬规则（违反即打回，无例外）

1. **状态转移只有一个写入者。** 一切 Redis 写路径必须收敛到 `scripts/*.lua`，经 `internal/rdb` 加载执行。禁止在 Go 代码里内联 eval 字符串；禁止业务代码绕过 `Queue` 引擎 API 直接 `HSET/ZADD/DEL` 任务状态。
2. **不改状态机语义。** 不得新增停留态（**没有 FAILED 停留态**——fail 是事件，见 design.md §0）；不得删除脚本中的任何守卫（`EXISTS` / `state` / `owner` / `ver` 检查都是竞态防线，不是冗余代码）。改脚本必须三处同步（§5）。
3. **fencing 纪律。** `ver` 只在 reserve 颁发、只在 sweep / fail-retry 失效。不得添加跳过 token 校验的"快捷路径"或"性能优化"。任何脚本改动后 **T4 必须重跑**。
4. **语义诚实。** 永远不实现、不声称 exactly-once。看到"顺手修好重复投递"的改动要停下来质疑——正确的位置是消费侧 `Dedup` 中间件，不是队列。
5. **时间纪律。** 一切到期/超时/排序判断用 Redis 服务器时间（脚本内 `TIME`）。Go 代码禁止用 `time.Now()` 参与正确性决策（日志、metrics 除外）。
6. **key 布局不可破坏。** hash tag `{qk:<name>}` 保证同队列同 slot（Cluster 前提）；task id 必须过 `idRe` 校验后才允许拼进 key（§go-implementation.md 2.5）。dedup 键不带 tag，这是故意的。
7. **依赖冻结。** 不引入新框架/队列库/工具包；确需第三方依赖，先在 go-implementation.md §0"依赖策略"记录理由并改此处文档，再动 go.mod。
8. **`ErrFenced` 不可吞。** 任何 complete/fail/heartbeat 调用点收到 `ERR_FENCED`：走指标 + warn 日志，然后**放弃**（租约已易主）。禁止重试、禁止当作普通错误上抛、禁止静默降级为成功。

## 3. 高危陷阱（历史勘误与故意设计，勿"好心修复"）

| 陷阱 | 正确认知 |
|---|---|
| heartbeat.lua 里 `ZADD lease dl <member>` 的 member 写成 token | **勘误过的 bug**：member 恒为 task_id，否则 sweep 永远扫不到被续期的任务（go-implementation.md §3.5）。看到旧写法是修 bug，不是改行为 |
| complete 不检查租约是否过期，只检查 owner+token | **故意的**：脚本域内与 sweep 线性化，先到先赢（design.md §5.4）。别给它加 `now > deadline` 拒绝逻辑 |
| `Terminator` 里终结写用 `context.WithoutCancel` | **必须的**：complete/fail 的落盘不能因 handler 超时半途而废（半途 = 丢响应 = 重投） |
| draining 时 handler 返回真实错误仍走 `Fail` | **正确的**：只有 `draining && ctx.Err() != nil` 才 Release。DB 超时不许伪装成下线 |
| miniredis 跑过了脚本用例 | **不算数**：其 Lua 子集不覆盖 TIME/效果复制语义。脚本正确性只在真实 Redis（testcontainers）上成立 |
| Priority 档位、MaxRetries 上限等没有代码强制 | **是有意的契约**：文档化的运营纪律（design.md §11），不要加"贴心"的运行时强校验除非文档同步 |

## 4. 代码风格

- `gofumpt` + `golangci-lint` 默认集，提交前干净。
- 注释只写**约束与不变量（为什么）**，不复述代码做什么；设计决策写进文档，不写进注释。
- 错误处理：脚本 STATUS → `errors.go` 哨兵 → 调用方 `errors.Is`。禁止字符串匹配错误、禁止 `fmt.Errorf` 伪造哨兵语义。
- 公共 API 首参 `ctx`；内部 goroutine 必须有明确退出路径（ctx 或 done channel），并处于 `-race` 测试覆盖下。
- 命名跟随文档词汇表：reserve/lease/vis/fence/sweep/promote/abandon——保持代码、文档、日志三者同词。

## 5. 文档同步义务（Definition of Done 的一部分）

**行为变更 = 三处同步，缺一即未完成：**

```
design.md（规范） → scripts/（实现） → integration/（验证）
```

只改脚本不改文档，或只改文档不动脚本与用例，均视为未完成。文档间冲突按 §0 裁决顺序修复。

## 6. 测试要求（按改动影响面选择，全部通过才算完成）

| 改动 | 必跑 |
|---|---|
| 纯 Go 逻辑（backoff/middleware/CLI） | 相关单测 + `go test -race ./...` |
| 任何脚本 / 状态机语义 | 对应黄金用例 + 全量 integration + `-count=10`；T4 必跑 |
| Worker 运行时（loop/heartbeat/shutdown） | T3 / T4 / T8 / T9 / T13 |
| 新功能 | ≥1 条新黄金用例 + 文档同步 |
| 性能敏感路径 | `-bench` 对照基线，回退 >20% 必须书面说明或回退改动 |

新增并发代码必须自带一条能在 `-race` 下复现其竞态的用例——写不出用例，说明还没想清楚竞态在哪。

## 7. 提交纪律

commit message 用祈使句、正文回答"为什么"；PR 描述附自查清单：

- [ ] `go test -race ./...` 通过
- [ ] 涉及脚本：integration 黄金用例通过（-count=10）
- [ ] 行为变化已三处同步（design.md / scripts / integration）
- [ ] 无新增依赖（或已在 §0 依赖策略记录理由）
- [ ] 无内联 eval、无绕过引擎的写路径、无被吞的 `ErrFenced`
- [ ] 环境受限未跑的部分已如实声明

## 8. 优先级与未覆盖场景

规则冲突时按此序裁决：**状态机完整性 > 语义诚实（at-least-once）> 测试覆盖 > 性能 > 代码优雅。**

遇到本文件未覆盖的场景：优先对照 design.md 的竞态消灭矩阵与转移表推演；仍无法判定时，**显式提出疑问并停下**，不要默默选择一条路。本仓库的第一美德是诚实——对自己不懂的东西诚实，对系统保证不了的东西诚实。
