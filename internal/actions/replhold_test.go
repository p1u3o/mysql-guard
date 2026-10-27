package actions

import (
	"testing"
	"time"

	"mysql-guard/internal/consul"
	"mysql-guard/internal/replhealth"
)

func TestTransitionActive(t *testing.T) {
	now := time.Now()
	if TransitionActive(nil, now) {
		t.Fatal("nil")
	}
	if !TransitionActive(&consul.Transition{Started: now.Add(-10 * time.Second)}, now) {
		t.Fatal("running")
	}
	if TransitionActive(&consul.Transition{Started: now.Add(-3 * time.Minute)}, now) {
		t.Fatal("stale running marker must expire")
	}
	if !TransitionActive(&consul.Transition{Started: now.Add(-30 * time.Second), Ended: now.Add(-5 * time.Second)}, now) {
		t.Fatal("grace")
	}
	if TransitionActive(&consul.Transition{Started: now.Add(-60 * time.Second), Ended: now.Add(-30 * time.Second)}, now) {
		t.Fatal("after grace")
	}
}

func TestFilterThreadAlerts(t *testing.T) {
	fs := map[string]time.Time{}
	t0 := time.Now()
	al := []replhealth.Alert{{Code: "repl_io_stopped"}, {Code: "repl_sql_stopped"}, {Code: "primary_read_only"}, {Code: "read_only_drift"}, {Code: "repl_lag"}}
	if got := filterThreadAlerts(al, fs, true, 45*time.Second, t0); len(got) != 0 {
		t.Fatalf("held: %v", got)
	}
	if got := filterThreadAlerts(al, fs, false, 45*time.Second, t0.Add(30*time.Second)); len(got) != 0 {
		t.Fatalf("within delay after hold: %v", got)
	}
	if got := filterThreadAlerts(al, fs, false, 45*time.Second, t0.Add(80*time.Second)); len(got) != 5 {
		t.Fatalf("persistent stop must alert: %v", got)
	}
	filterThreadAlerts(nil, fs, false, 45*time.Second, t0.Add(81*time.Second))
	if len(fs) != 0 {
		t.Fatal("cleared codes must reset")
	}
}

func TestFailoverFollowup(t *testing.T) {
	now := time.Now()
	f := &consul.Transition{Action: "failover", From: "a", To: "b", Started: now.Add(-time.Minute)}
	if !failoverFollowup(f, "a", now) {
		t.Fatal("running failover: want fold")
	}
	if failoverFollowup(f, "b", now) || failoverFollowup(nil, "a", now) {
		t.Fatal("other node / no marker: want alert")
	}
	f.Ended = now.Add(-10 * time.Minute)
	if !failoverFollowup(f, "a", now) {
		t.Fatal("ended 10m ago: want fold")
	}
	f.Ended = now.Add(-31 * time.Minute)
	if failoverFollowup(f, "a", now) {
		t.Fatal("ended 31m ago: want alert")
	}
	s := &consul.Transition{Action: "switchover", From: "a", To: "b", Started: now.Add(-time.Minute)}
	if failoverFollowup(s, "a", now) {
		t.Fatal("switchover: not a failover follow-up")
	}
	if !fenceFolded(s, "b", now) || fenceFolded(s, "c", now) {
		t.Fatal("active switchover: fold b, not c")
	}
	s.Ended = now.Add(-30 * time.Second)
	if fenceFolded(s, "b", now) {
		t.Fatal("switchover ended 30s ago: want alert")
	}
}
