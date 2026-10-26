package actions

import (
	"context"
	"time"

	"mysql-guard/internal/actionlog"
	"mysql-guard/internal/consul"
)

// Logger writes to Consul action log KV.
type Logger struct {
	store *consul.Store
	guard string
	max   int
}

func NewLogger(store *consul.Store, guard string, max int) *Logger {
	return &Logger{store: store, guard: guard, max: max}
}

func (l *Logger) Log(ctx context.Context, epoch uint64, action, reason, result, step string) {
	if l == nil || l.store == nil {
		return
	}
	_ = l.store.AppendActionLog(ctx, actionlog.Entry{
		Timestamp: time.Now().UTC(),
		Epoch:     epoch,
		Guard:     l.guard,
		Action:    action,
		Reason:    reason,
		Result:    result,
		Step:      step,
	}, l.max)
}
