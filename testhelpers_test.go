package kiki

import (
	"fmt"
	"log/slog"
	"math/rand"

	"github.com/garfield-dev-team/kiki/internal/metrics"
)

var nopMetrics = metrics.NewNoop()

func rngForTest() *rand.Rand {
	return rand.New(rand.NewSource(42))
}

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func taskID(i int) string { return fmt.Sprintf("task-%03d", i) }
