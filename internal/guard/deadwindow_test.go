package guard

import (
	"testing"
	"time"
)

const (
	tInterval = 2 * time.Second
	tDead     = 45 * time.Second
)

var tMaxGap = VoteMaxAge(tInterval, 3*time.Second)

// run feeds one quorum observation per interval for d (inclusive of the first
// tick) starting at t0 and returns the first time a failover would be allowed
// (zero if never) and the time of the last tick.
func run(w *deadWindow, t0 time.Time, d time.Duration, met bool, deadAfter time.Duration) (fireAt, last time.Time) {
	for el := time.Duration(0); el <= d; el += tInterval {
		now := t0.Add(el)
		w.observe(now, met, tMaxGap)
		if met && fireAt.IsZero() && w.ready(now, deadAfter) {
			fireAt = now
		}
		last = now
	}
	return fireAt, last
}

func TestDeadWindowBlipShorterThanDeadAfterNeverFailsOver(t *testing.T) {
	var w deadWindow
	t0 := time.Date(2026, 11, 4, 15, 2, 27, 0, time.UTC)
	// 15 s of dead votes (the 2026-11-04 incident was about 10-15 s): many
	// more than 3 consecutive ticks, still well under -dead-after.
	fire, last := run(&w, t0, 15*time.Second, true, tDead)
	if !fire.IsZero() {
		t.Fatalf("failover allowed after a 15s blip at %s", fire.Sub(t0))
	}
	if w.ticks < minDeadTicks {
		t.Fatalf("test must exceed %d ticks, got %d", minDeadTicks, w.ticks)
	}
	// Primary answers again: window resets.
	w.observe(last.Add(tInterval), false, tMaxGap)
	if w.ticks != 0 || !w.since.IsZero() || w.elapsed(last.Add(tInterval)) != 0 {
		t.Fatalf("window not reset on recovery: %+v", w)
	}
}

func TestDeadWindowSustainedDeathFailsOverAfterDeadAfter(t *testing.T) {
	var w deadWindow
	t0 := time.Date(2026, 11, 4, 15, 0, 0, 0, time.UTC)
	fire, _ := run(&w, t0, 2*time.Minute, true, tDead)
	if fire.IsZero() {
		t.Fatal("sustained death never allowed a failover")
	}
	if got := fire.Sub(t0); got < tDead || got >= tDead+tInterval {
		t.Fatalf("failover allowed after %s, want first tick at or after %s", got, tDead)
	}
}

func TestDeadWindowNeedsMinTicksEvenWithTinyDeadAfter(t *testing.T) {
	var w deadWindow
	t0 := time.Unix(1000, 0)
	for i := 0; i < minDeadTicks-1; i++ {
		now := t0.Add(time.Duration(i) * tInterval)
		w.observe(now, true, tMaxGap)
		if w.ready(now, 0) {
			t.Fatalf("ready after only %d ticks", i+1)
		}
	}
	now := t0.Add(time.Duration(minDeadTicks-1) * tInterval)
	w.observe(now, true, tMaxGap)
	if !w.ready(now, 0) {
		t.Fatal("not ready after minimum ticks with dead-after=0 (old behaviour)")
	}
}

func TestDeadWindowRecoveryResetsTimer(t *testing.T) {
	var w deadWindow
	t0 := time.Date(2026, 11, 4, 15, 0, 0, 0, time.UTC)
	// 40 s dead, one healthy tick, then 40 s dead again: 80 s total but never
	// 45 s continuously.
	if fire, last := run(&w, t0, 40*time.Second, true, tDead); !fire.IsZero() {
		t.Fatalf("early failover at %s", fire.Sub(t0))
	} else {
		w.observe(last.Add(tInterval), false, tMaxGap)
		t1 := last.Add(2 * tInterval)
		if fire, _ := run(&w, t1, 40*time.Second, true, tDead); !fire.IsZero() {
			t.Fatalf("failover allowed %s into the second run; timer was not reset", fire.Sub(t1))
		}
		// ...but the second run keeps going and fires 45 s after its own start.
		fire, _ := run(&w, t1.Add(42*time.Second), 10*time.Second, true, tDead)
		if fire.IsZero() || fire.Sub(t1) < tDead {
			t.Fatalf("second run fired at %v, want >= %s after its start", fire.Sub(t1), tDead)
		}
	}
}

func TestDeadWindowQuorumDipResets(t *testing.T) {
	// One voter flickering back (quorum below -votes-needed for one tick)
	// restarts the clock, like any healthy observation.
	var w deadWindow
	t0 := time.Unix(5000, 0)
	run(&w, t0, 30*time.Second, true, tDead)
	w.observe(t0.Add(32*time.Second), false, tMaxGap)
	if w.ticks != 0 {
		t.Fatalf("ticks = %d after dip", w.ticks)
	}
	w.observe(t0.Add(34*time.Second), true, tMaxGap)
	if got := w.elapsed(t0.Add(34 * time.Second)); got != 0 {
		t.Fatalf("elapsed after dip = %s, want 0", got)
	}
}

func TestDeadWindowGapRestartsWindow(t *testing.T) {
	// A leader that was away (lost the lock, maintenance mode, read-only
	// grace) cannot vouch for the time it did not observe.
	var w deadWindow
	t0 := time.Unix(9000, 0)
	_, last := run(&w, t0, 30*time.Second, true, tDead)
	back := last.Add(tMaxGap + time.Second)
	w.observe(back, true, tMaxGap)
	if w.ticks != 1 || w.elapsed(back) != 0 {
		t.Fatalf("window not restarted after gap: ticks=%d elapsed=%s", w.ticks, w.elapsed(back))
	}
	if w.ready(back.Add(20*time.Second), tDead) {
		t.Fatal("stale pre-gap observations counted toward -dead-after")
	}
}

func TestDeadWindowForceBypassesDeadAfter(t *testing.T) {
	// A reachable read-only primary that is missing data already waited out
	// -read-only-grace; -dead-after must not delay it further.
	var w deadWindow
	now := time.Unix(1, 0)
	w.force(now)
	if !w.ready(now, tDead) {
		t.Fatal("forced window not ready")
	}
	w.observe(now.Add(tInterval), false, tMaxGap)
	if w.ready(now.Add(tInterval), tDead) {
		t.Fatal("force survived a healthy observation")
	}
}

func TestDeadWindowResetAfterFailover(t *testing.T) {
	var w deadWindow
	t0 := time.Unix(100, 0)
	run(&w, t0, 60*time.Second, true, tDead)
	w.reset()
	if w.ready(t0.Add(61*time.Second), tDead) || w.ticks != 0 {
		t.Fatalf("reset left state: %+v", w)
	}
}
