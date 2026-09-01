package kiki

import "embed"

// scripts/*.lua 是一切状态转移的唯一规范实现（go-implementation.md §0.3）。
// 以 go:embed 编译进二进制，版本随库走；Go 代码中不允许出现任何内联 eval 字符串。
//
//go:embed scripts/*.lua
var scriptFS embed.FS
