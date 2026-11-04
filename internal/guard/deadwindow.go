package guard

import "time"

// minDeadTicks is the number of consecutive leader ticks the failover quorum
// must hold before a failover is considered.
const minDeadTicks = 3

// deadWindow tracks how long the failover quorum (>= -votes-needed dead votes)
// has held continuously. A failover needs both minDeadTicks consecutive ticks
// and the quorum to have held for at least -dead-after: any tick without the
// quorum (a healthy observation) resets the window, so a network blip or a
// stall shorter than -dead-after can never fail the primary over.
type deadWindow struct {
	ticks  int       // consecutive ticks with the quorum met
	since  time.Time // first tick of the current run (zero = not running)
	last   time.Time // time of the last observation
	forced bool      // read-only primary already past -read-only-grace
}

// observe records one leader tick. met is whether the quorum held on this
// tick. maxGap is the longest pause between observations that still counts as
// continuous: a leader that was away (lost the lock, maintenance mode, a
// read-only grace period) cannot vouch for the time it did not observe, so a
// longer gap restarts the window. It returns the consecutive tick count.
func (w *deadWindow) observe(now time.Time, met bool, maxGap time.Duration) int {
	if !w.last.IsZero() && now.Sub(w.last) > maxGap {
		w.reset()
	}
	w.last = now
	if !met {
		w.reset()
		w.last = now
		return 0
	}
	if w.ticks == 0 {
		w.since = now
	}
	w.ticks++
	return w.ticks
}

// elapsed is how long the quorum has held, measured from the first tick of the
// current run.
func (w *deadWindow) elapsed(now time.Time) time.Duration {
	if w.ticks == 0 || w.since.IsZero() {
		return 0
	}
	return now.Sub(w.since)
}

// ready reports whether a failover may proceed: enough consecutive ticks and
// the quorum held for at least deadAfter.
func (w *deadWindow) ready(now time.Time, deadAfter time.Duration) bool {
	if w.forced {
		return true
	}
	return w.ticks >= minDeadTicks && w.elapsed(now) >= deadAfter
}

func (w *deadWindow) reset() {
	w.ticks = 0
	w.forced = false
	w.since = time.Time{}
}

// force marks the window as satisfied without waiting for -dead-after: a
// reachable read-only primary that is missing data has already waited out
// -read-only-grace, which is its own timer.
func (w *deadWindow) force(now time.Time) {
	w.ticks = minDeadTicks
	w.forced = true
	w.last = now
}
