package replhealth

import (
	"fmt"
	"time"
)

// State is replication-related signals from one node.
type State struct {
	Node           string
	IOThreadOn     bool
	SQLThreadOn    bool
	ReadOnly       bool
	ExpectReadOnly bool
	LagSeconds     float64
}

// Alert is a replication hygiene issue.
type Alert struct {
	Severity string // warning, critical
	Code     string
	Message  string
}

// Config thresholds.
type Config struct {
	MaxLagSeconds float64
}

// Evaluate returns alerts for IO/SQL stopped, lag, and read_only drift.
func Evaluate(st State, cfg Config) []Alert {
	var out []Alert
	if !st.IOThreadOn {
		out = append(out, Alert{Severity: "critical", Code: "repl_io_stopped", Message: fmt.Sprintf("%s: IO thread not running", st.Node)})
	}
	if !st.SQLThreadOn {
		out = append(out, Alert{Severity: "critical", Code: "repl_sql_stopped", Message: fmt.Sprintf("%s: SQL thread not running", st.Node)})
	}
	if cfg.MaxLagSeconds > 0 && st.LagSeconds > cfg.MaxLagSeconds {
		out = append(out, Alert{Severity: "warning", Code: "repl_lag", Message: fmt.Sprintf("%s: lag %.1fs over %.1fs", st.Node, st.LagSeconds, cfg.MaxLagSeconds)})
	}
	if st.ExpectReadOnly && !st.ReadOnly {
		out = append(out, Alert{Severity: "critical", Code: "read_only_drift", Message: fmt.Sprintf("%s: expected read_only", st.Node)})
	}
	if !st.ExpectReadOnly && st.ReadOnly && st.Node != "" {
		// primary should be writable
		out = append(out, Alert{Severity: "warning", Code: "primary_read_only", Message: fmt.Sprintf("%s: primary is read_only", st.Node)})
	}
	return out
}

// StaleHeartbeat returns true when heartbeat and last queued are both over threshold.
func StaleHeartbeat(heartbeatAge, lastQueuedAge time.Duration, threshold time.Duration) bool {
	return heartbeatAge > threshold && lastQueuedAge > threshold
}
