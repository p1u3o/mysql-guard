package guard

import (
	"fmt"
	"time"
)

// ---- primary recovery hold (v0.3.7) ----
//
// While the current primary is unreachable or read-only, the leader either
// makes it writable again (restore_writer) or fails over within
// max(DeadAfter, ReadOnlyGrace) plus a vote round. The replica-side symptoms
// of that (repl_* on the replica, proxysql_writer_mismatch) are the same
// event and are held, so it sends exactly one alert. The hold is bounded:
// past the window, or VoteMaxAge after the primary is healthy again at the
// same epoch, alerts flow as usual.

type primaryHold struct {
	key     string    // "<primary>|<epoch>" being observed
	since   time.Time // first tick the primary was down or read-only (zero = healthy)
	clearAt time.Time // tick the primary became healthy again after trouble
}

// observePrimary records whether primary looks down or read-only this tick.
func (r *Runtime) observePrimary(now time.Time, primary string, epoch uint64, down, readOnly bool) {
	key := fmt.Sprintf("%s|%d", primary, epoch)
	r.mu.Lock()
	defer r.mu.Unlock()
	h := &r.pHold
	if h.key != key {
		// epoch bump (restore_writer / failover / switchover): keep the
		// post-recovery grace from the previous epoch's trouble.
		if !h.since.IsZero() {
			h.clearAt = now
		}
		h.key, h.since = key, time.Time{}
	}
	if primary == "" {
		return
	}
	if down || readOnly {
		if h.since.IsZero() {
			h.since = now
		}
		return
	}
	if !h.since.IsZero() {
		h.clearAt = now
		h.since = time.Time{}
	}
}

// recoveryHolding reports whether replica-side alerts are held at now.
func (r *Runtime) recoveryHolding(now time.Time) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return recoveryHold(r.pHold, now, r.cfg.DeadAfter, r.cfg.ReadOnlyGrace, VoteMaxAge(r.cfg.Interval, r.cfg.ProbeTimeout))
}

func recoveryHold(h primaryHold, now time.Time, deadAfter, roGrace, vote time.Duration) bool {
	if !h.since.IsZero() {
		w := deadAfter
		if roGrace > w {
			w = roGrace
		}
		return now.Sub(h.since) <= w+2*vote
	}
	return !h.clearAt.IsZero() && now.Sub(h.clearAt) <= vote
}
