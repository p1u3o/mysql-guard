package decision

import (
	"time"

	"mysql-guard/internal/gtid"
	"mysql-guard/internal/vote"
)

// ScenarioInput bundles state for failure-scenario evaluation (table-driven tests).
type ScenarioInput struct {
	Name           string
	Now            time.Time
	Ballot         vote.Ballot
	VotesNeeded    int
	PrimaryWritable bool
	ReplicaLag     ReplicaState
	PeerGTID       map[string]gtid.Set
	LocalEpoch     uint64
	RemoteEpoch    uint64
	ConsulUp       bool
	Maintenance    bool
	IsLeader       bool
	Mode           string
	LastFailover   time.Time
	MinBetween     time.Duration
}

// ScenarioResult is the high-level action the engine would take.
type ScenarioResult struct {
	Action string // none, warn, promote, fence, alert
	Reason string
}

// EvaluateScenario maps plan failure scenarios to expected behaviour (decision layer only).
func EvaluateScenario(in ScenarioInput) ScenarioResult {
	if !in.ConsulUp {
		return ScenarioResult{Action: "none", Reason: "consul outage: no promotion"}
	}
	if in.Maintenance {
		return ScenarioResult{Action: "none", Reason: "maintenance mode: actions paused"}
	}
	if !in.IsLeader {
		return ScenarioResult{Action: "none", Reason: "not leader"}
	}

	dead := vote.PrimaryDead(in.Ballot, in.VotesNeeded)
	deadN := in.Ballot.DeadCount()

	if in.PrimaryWritable && in.RemoteEpoch > 0 && in.LocalEpoch < in.RemoteEpoch {
		return ScenarioResult{Action: "fence", Reason: "stale epoch writer"}
	}
	if in.PrimaryWritable && deadN < in.VotesNeeded {
		if in.LocalEpoch != in.RemoteEpoch && in.LocalEpoch > 0 && in.RemoteEpoch > 0 {
			return ScenarioResult{Action: "alert", Reason: "split-brain ambiguous"}
		}
	}

	if deadN == 1 {
		return ScenarioResult{Action: "warn", Reason: "1/3 dead votes"}
	}
	if deadN >= 1 && deadN < in.VotesNeeded {
		return ScenarioResult{Action: "warn", Reason: "partition: insufficient dead votes"}
	}

	if dead && in.LastFailover.Add(in.MinBetween).After(in.Now) {
		return ScenarioResult{Action: "none", Reason: "failover rate limited"}
	}

	if dead {
		gate := EvaluatePromotionGate(in.Now, in.ReplicaLag, GateConfig{})
		if !gate.OK {
			return ScenarioResult{Action: "alert", Reason: gate.Reason}
		}
		_, ok := PickCandidate(in.PeerGTID)
		if !ok {
			return ScenarioResult{Action: "alert", Reason: "no peer candidate"}
		}
		return ScenarioResult{Action: "promote", Reason: "primary dead, gtid gate pass"}
	}

	return ScenarioResult{Action: "none", Reason: "healthy"}
}
