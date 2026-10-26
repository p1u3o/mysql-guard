package rejoin

import (
	"fmt"

	"mysql-guard/internal/errant"
	"mysql-guard/internal/gtid"
)

// Outcome classifies whether an old primary can rejoin or needs rebuild.
type Outcome string

const (
	OutcomeAutoPosition Outcome = "auto-position"
	OutcomeNeedsRebuild Outcome = "needs-rebuild"
	OutcomeBlockedErrant Outcome = "blocked-errant"
)

// Plan is the recommended repair for a returning node.
type Plan struct {
	Outcome Outcome
	Reason  string
	Alert   string
}

// Evaluate decides rejoin path without flashback (plan: auto-position or rebuild).
func Evaluate(localExecuted, clusterPrimaryExecuted gtid.Set, readOnly bool) Plan {
	err := errant.Detect(localExecuted, clusterPrimaryExecuted)
	if err.Errant {
		return Plan{
			Outcome: OutcomeBlockedErrant,
			Reason:  err.Reason,
			Alert:   "rejoin blocked: errant GTID on " + string(err.ErrantSet),
		}
	}
	missing := gtid.Subtract(clusterPrimaryExecuted, localExecuted)
	if !missing.Empty() {
		if readOnly {
			return Plan{
				Outcome: OutcomeAutoPosition,
				Reason:  "read_only node can CHANGE REPLICATION SOURCE TO ... SOURCE_AUTO_POSITION=1",
			}
		}
		return Plan{
			Outcome: OutcomeNeedsRebuild,
			Reason:  "writable node missing cluster transactions; rebuild required",
			Alert:   fmt.Sprintf("needs-rebuild: missing %s", missing),
		}
	}
	return Plan{
		Outcome: OutcomeAutoPosition,
		Reason:  "GTID sets aligned; auto-position rejoin",
	}
}
