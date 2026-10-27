package actions

import (
	"context"
	"time"

	"mysql-guard/internal/consul"
	"mysql-guard/internal/replhealth"
)

// Replica-thread alerts (repl_io_stopped / repl_sql_stopped) are expected while
// a switchover or failover re-points replication. v0.3.2 holds them back:
//   - while a transition marker is active (not ended, started < TransitionMaxAge
//     ago) or ended less than TransitionGrace ago, on any node;
//   - and until the condition has persisted for ReplThreadAlertDelay, so the
//     replica-side symptom of a dead primary does not alert ahead of the
//     failover itself. A real, persistent stop still alerts after the delay.
const (
	TransitionMaxAge = 2 * time.Minute
	TransitionGrace  = 20 * time.Second
)

func isThreadCode(c string) bool { return c == "repl_io_stopped" || c == "repl_sql_stopped" }

// TransitionActive reports whether t should hold thread alerts at now.
func TransitionActive(t *consul.Transition, now time.Time) bool {
	if t == nil {
		return false
	}
	if t.Ended.IsZero() {
		return now.Sub(t.Started) < TransitionMaxAge
	}
	return now.Sub(t.Ended) < TransitionGrace
}

// filterThreadAlerts drops held thread alerts. firstSeen is updated in place.
func filterThreadAlerts(alerts []replhealth.Alert, firstSeen map[string]time.Time, held bool, delay time.Duration, now time.Time) []replhealth.Alert {
	present := map[string]bool{}
	out := alerts[:0:0]
	for _, a := range alerts {
		if !isThreadCode(a.Code) {
			out = append(out, a)
			continue
		}
		present[a.Code] = true
		if held {
			firstSeen[a.Code] = now // delay restarts after the transition
			continue
		}
		fs, ok := firstSeen[a.Code]
		if !ok {
			firstSeen[a.Code] = now
			fs = now
		}
		if now.Sub(fs) >= delay {
			out = append(out, a)
		}
	}
	for c := range firstSeen {
		if !present[c] {
			delete(firstSeen, c)
		}
	}
	return out
}

func (e *Executor) transitionHeld(ctx context.Context) bool {
	if e.store == nil {
		return false
	}
	t, err := e.store.GetTransition(ctx)
	if err != nil {
		e.log.Warn("transition kv read", "err", err)
		return false
	}
	return TransitionActive(t, time.Now())
}

func (e *Executor) markTransition(ctx context.Context, action, from, to string) func() {
	if e.store == nil {
		return func() {}
	}
	t := consul.Transition{Action: action, From: from, To: to, By: e.cfg.NodeName, Started: time.Now().UTC()}
	if err := e.store.PutTransition(ctx, t); err != nil {
		e.log.Warn("transition kv write", "err", err)
	}
	return func() {
		t.Ended = time.Now().UTC()
		if err := e.store.PutTransition(context.WithoutCancel(ctx), t); err != nil {
			e.log.Warn("transition kv write", "err", err)
		}
	}
}
