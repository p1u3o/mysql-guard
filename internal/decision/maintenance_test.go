package decision

import (
	"testing"
	"time"

	"mysql-guard/internal/vote"
)

func TestMaintenancePausesActions(t *testing.T) {
	now := time.Now()
	b := vote.Ballot{}
	b.UpdateAll(now, vote.ProbeFailed, vote.ProbeFailed, vote.ProbeFailed, 1, 0)
	in := ScenarioInput{
		Now: now, Ballot: b, VotesNeeded: 2, ConsulUp: true, IsLeader: true,
		Maintenance: true, MinBetween: time.Hour, LastFailover: now.Add(-2 * time.Hour),
	}
	r := EvaluateScenario(in)
	if r.Action != "none" || r.Reason != "maintenance mode: actions paused" {
		t.Fatalf("%+v", r)
	}
}
