package decision

import (
	"time"

	"mysql-guard/internal/gtid"
)

// ReplicaState is input for the GTID promotion gate.
type ReplicaState struct {
	Node            string
	ReadOnly        bool
	IOThreadRunning bool
	SQLThreadKnown  bool
	Executed        gtid.Set
	Retrieved       gtid.Set
	PrimaryExecuted gtid.Set
	ApplyLag        time.Duration
	MissingSince    time.Time
}

// GateConfig mirrors lag-gate flags.
type GateConfig struct {
	MaxApplyLag    time.Duration
	LagGateMaxWait time.Duration
}

// GateResult explains promote / block.
type GateResult struct {
	OK           bool
	Reason       string
	Missing      gtid.Set
	BlockedByLag bool
}

// EvaluatePromotionGate implements the plan's GTID gate (default never promote with missing trx).
func EvaluatePromotionGate(now time.Time, st ReplicaState, cfg GateConfig) GateResult {
	if !st.ReadOnly {
		return GateResult{Reason: "not read_only"}
	}
	if !st.IOThreadRunning || !st.SQLThreadKnown {
		return GateResult{Reason: "replication threads unknown"}
	}
	missing := gtid.Subtract(st.PrimaryExecuted, st.Executed)
	if !missing.Empty() {
		retMissing := gtid.Subtract(st.Retrieved, st.Executed)
		if retMissing.Empty() {
			if cfg.LagGateMaxWait == 0 {
				return GateResult{Reason: "missing executed GTIDs", Missing: missing, BlockedByLag: true}
			}
			if st.MissingSince.IsZero() {
				return GateResult{Reason: "missing GTIDs, waiting", Missing: missing, BlockedByLag: true}
			}
			if now.Sub(st.MissingSince) > cfg.LagGateMaxWait {
				return GateResult{Reason: "lag gate max wait exceeded", Missing: missing, BlockedByLag: true}
			}
			return GateResult{Reason: "waiting for GTID catch-up", Missing: missing, BlockedByLag: true}
		}
		if st.ApplyLag > cfg.MaxApplyLag {
			return GateResult{Reason: "apply lag over max", Missing: missing, BlockedByLag: true}
		}
	}
	return GateResult{OK: true, Reason: "gtid gate pass"}
}

// PickCandidate runs offset election across peers.
func PickCandidate(peerExecuted map[string]gtid.Set) (string, bool) {
	return gtid.ComparePeers(peerExecuted)
}
