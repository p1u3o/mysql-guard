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
	al := []replhealth.Alert{{Code: "repl_io_stopped"}, {Code: "repl_sql_stopped"}, {Code: "repl_lag"}}
	if got := filterThreadAlerts(al, fs, true, 45*time.Second, t0); len(got) != 1 || got[0].Code != "repl_lag" {
		t.Fatalf("held: %v", got)
	}
	if got := filterThreadAlerts(al, fs, false, 45*time.Second, t0.Add(30*time.Second)); len(got) != 1 {
		t.Fatalf("within delay after hold: %v", got)
	}
	if got := filterThreadAlerts(al, fs, false, 45*time.Second, t0.Add(80*time.Second)); len(got) != 3 {
		t.Fatalf("persistent stop must alert: %v", got)
	}
	filterThreadAlerts(nil, fs, false, 45*time.Second, t0.Add(81*time.Second))
	if len(fs) != 0 {
		t.Fatal("cleared codes must reset")
	}
}
