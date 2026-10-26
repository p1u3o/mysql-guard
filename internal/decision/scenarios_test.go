package decision

import (
	"testing"
	"time"

	"mysql-guard/internal/gtid"
	"mysql-guard/internal/vote"
)

func u() gtid.Set {
	return gtid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100")
}

func deadBallot(now time.Time, _ time.Duration) vote.Ballot {
	b := vote.Ballot{}
	b.UpdateAll(now, vote.ProbeFailed, vote.ProbeFailed, vote.ProbeFailed, 1, 0)
	return b
}

func partialBallot(now time.Time, _ time.Duration) vote.Ballot {
	b := vote.Ballot{}
	b.UpdateAll(now, vote.ProbeFailed, vote.ProbeOK, vote.ProbeOK, 1, 0)
	return b
}

func replicaOK() ReplicaState {
	set := u()
	return ReplicaState{
		Node:            "db-b",
		ReadOnly:        true,
		IOThreadRunning: true,
		SQLThreadKnown:  true,
		Executed:        set,
		Retrieved:       set,
		PrimaryExecuted: set,
	}
}

func TestFailureScenarios(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	deadAfter := 30 * time.Second
	base := func() ScenarioInput {
		return ScenarioInput{
			Now:          now,
			VotesNeeded:  2,
			ConsulUp:     true,
			IsLeader:     true,
			MinBetween:   time.Hour,
			LastFailover: now.Add(-2 * time.Hour),
			ReplicaLag:   replicaOK(),
			PeerGTID: map[string]gtid.Set{
				"db-a": u(),
				"db-b": u(),
			},
		}
	}

	cases := []struct {
		name   string
		mutate func(*ScenarioInput)
		want   string
	}{
		{
			name: "primary crash",
			mutate: func(in *ScenarioInput) {
				in.Ballot = deadBallot(now, deadAfter)
			},
			want: "promote",
		},
		{
			name: "primary hung",
			mutate: func(in *ScenarioInput) {
				in.Ballot = deadBallot(now, deadAfter)
			},
			want: "promote",
		},
		{
			name: "partition guard to primary only",
			mutate: func(in *ScenarioInput) {
				in.Ballot = partialBallot(now, deadAfter)
			},
			want: "warn",
		},
		{
			name: "primary isolated from all",
			mutate: func(in *ScenarioInput) {
				in.Ballot = deadBallot(now, deadAfter)
			},
			want: "promote",
		},
		{
			name: "consul outage",
			mutate: func(in *ScenarioInput) {
				in.ConsulUp = false
			},
			want: "none",
		},
		{
			name: "split-brain ambiguous",
			mutate: func(in *ScenarioInput) {
				in.PrimaryWritable = true
				in.LocalEpoch = 2
				in.RemoteEpoch = 3
				in.Ballot = partialBallot(now, deadAfter)
			},
			want: "fence",
		},
		{
			name: "stale epoch writer fenced",
			mutate: func(in *ScenarioInput) {
				in.PrimaryWritable = true
				in.LocalEpoch = 1
				in.RemoteEpoch = 5
			},
			want: "fence",
		},
		{
			name: "replica lagging missing GTIDs",
			mutate: func(in *ScenarioInput) {
				in.Ballot = deadBallot(now, deadAfter)
				in.ReplicaLag.Executed = gtid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-50")
				in.ReplicaLag.Retrieved = in.ReplicaLag.Executed
				in.ReplicaLag.PrimaryExecuted = u()
			},
			want: "alert",
		},
		{
			name: "both nodes down",
			mutate: func(in *ScenarioInput) {
				in.Ballot = deadBallot(now, deadAfter)
				in.ReplicaLag.IOThreadRunning = false
			},
			want: "alert",
		},
		{
			name: "not leader",
			mutate: func(in *ScenarioInput) {
				in.IsLeader = false
				in.Ballot = deadBallot(now, deadAfter)
			},
			want: "none",
		},
		{
			name: "failover rate limit",
			mutate: func(in *ScenarioInput) {
				in.Ballot = deadBallot(now, deadAfter)
				in.LastFailover = now.Add(-5 * time.Minute)
				in.MinBetween = time.Hour
			},
			want: "none",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := base()
			if tc.mutate != nil {
				tc.mutate(&in)
			}
			got := EvaluateScenario(in)
			if got.Action != tc.want {
				t.Fatalf("action=%q want %q reason=%s", got.Action, tc.want, got.Reason)
			}
		})
	}
}

func TestPromotionGateDefaultNeverLag(t *testing.T) {
	now := time.Now()
	st := replicaOK()
	st.Executed = gtid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-90")
	st.Retrieved = st.Executed
	st.PrimaryExecuted = u()
	r := EvaluatePromotionGate(now, st, GateConfig{LagGateMaxWait: 0})
	if r.OK || !r.BlockedByLag {
		t.Fatalf("expected blocked by lag, got %+v", r)
	}
}
