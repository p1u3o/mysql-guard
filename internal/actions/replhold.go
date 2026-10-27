package actions

import (
	"context"
	"time"

	"mysql-guard/internal/consul"
	"mysql-guard/internal/replhealth"
)

// Replication-health alerts (repl_io_stopped, repl_sql_stopped, primary_read_only,
// read_only_drift, repl_state_unknown, repl_lag, repl_stalled) are tripped
// transiently while a switchover or failover fences, promotes and re-points
// replication. v0.3.2 held back the thread codes; v0.3.3 holds every code:
//   - while a transition marker is active (not ended, started < TransitionMaxAge
//     ago) or ended less than TransitionGrace ago, on any node;
//   - and until the condition has persisted for ReplThreadAlertDelay, so the
//     replica-side symptom of a dead primary does not alert ahead of the
//     failover itself. A real, persistent stop still alerts after the delay.
const (
	TransitionMaxAge = 2 * time.Minute
	TransitionGrace  = 20 * time.Second
)

// isThreadCode: every replhealth code is transition-held since v0.3.3.
func isThreadCode(c string) bool { return c != "" }

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

// FenceFollowupWindow: a returning old primary of a failover that ended less
// than this long ago is fenced without a separate alert (the failover alert
// already reported it dead); the fence is still action-logged. Since v0.3.4 the
// same node's replication-health alerts (threads stopped, read_only, lag, ...)
// are also held for this window while it waits to be rejoined; if it is still
// unhealthy after the window they alert once (after ReplThreadAlertDelay).
// Overridable via -fence-followup-window (config.FenceFollowupWindow).
var FenceFollowupWindow = 30 * time.Minute

// fencedFollowupHeld reports whether node is the fenced old primary of a recent
// failover (within FenceFollowupWindow), whose replication-health alerts are held.
func (e *Executor) fencedFollowupHeld(ctx context.Context, node string) bool {
	if e.store == nil || node == "" {
		return false
	}
	t, err := e.store.GetTransition(ctx)
	if err != nil {
		return false
	}
	return failoverFollowup(t, node, time.Now())
}

func (e *Executor) fenceIsFailoverFollowup(ctx context.Context, node string) bool {
	if e.store == nil {
		return false
	}
	t, err := e.store.GetTransition(ctx)
	if err != nil {
		return false
	}
	return fenceFolded(t, node, time.Now())
}

// fenceFolded: the fence is part of a transition already alerted on — the old
// primary of a recent failover, or either node during an active switchover /
// failover (+grace), e.g. a target briefly writable before an abort.
func fenceFolded(t *consul.Transition, node string, now time.Time) bool {
	if failoverFollowup(t, node, now) {
		return true
	}
	return t != nil && (t.From == node || t.To == node) && TransitionActive(t, now)
}

// failoverFollowup: node is the old primary of a failover that is running or ended < FenceFollowupWindow ago.
func failoverFollowup(t *consul.Transition, node string, now time.Time) bool {
	if t == nil || t.Action != "failover" || t.From != node {
		return false
	}
	if t.Ended.IsZero() {
		return now.Sub(t.Started) < TransitionMaxAge
	}
	return now.Sub(t.Ended) < FenceFollowupWindow
}

func (e *Executor) fenceAlert(ctx context.Context, node, reason string) {
	if e.fenceIsFailoverFollowup(ctx, node) {
		e.log.Info("fence alert folded into transition alert", "node", node, "reason", reason)
		return
	}
	if e.alerts != nil {
		_ = e.alerts.Alert(ctx, "fence", reason, true)
	}
}

// anyFailoverFollowup: a failover ran or ended < FenceFollowupWindow ago.
func (e *Executor) anyFailoverFollowup(ctx context.Context) bool {
	if e.store == nil {
		return false
	}
	t, err := e.store.GetTransition(ctx)
	if err != nil || t == nil {
		return false
	}
	return failoverFollowup(t, t.From, time.Now())
}
