package guard

import (
	"time"

	"mysql-guard/internal/proxysql"
)

// NodeTopology is one DB peer in /status.
type NodeTopology struct {
	Node           string  `json:"node"`
	Reachable      bool    `json:"reachable"`
	Executed       string  `json:"gtid_executed"`
	Retrieved      string  `json:"gtid_retrieved"`
	LagSec         float64 `json:"lag_seconds"`
	ReadOnly       bool    `json:"read_only"`
	SuperReadOnly  bool    `json:"super_read_only"`
	ReplState      string  `json:"repl_state"`
	ReplError      string  `json:"repl_error,omitempty"`
	IOThread       *bool   `json:"io_thread"` // null when replication state is unknown
	SQLThread      *bool   `json:"sql_thread"`
	ProxySQLStatus string  `json:"proxysql_status,omitempty"`
	ProxySQLHG     int     `json:"proxysql_hostgroup,omitempty"`
}

// GuardTopology is a running guard instance.
type GuardTopology struct {
	Node          string    `json:"node"`
	Role          string    `json:"role"`
	Mode          string    `json:"mode"`
	Leader        bool      `json:"leader"`
	Follower      bool      `json:"follower"`
	Up            bool      `json:"up"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	LoopRenewSec  float64   `json:"loop_renew_age_sec"`
	LoopHealthSec float64   `json:"loop_health_age_sec"`
	LoopWatchSec  float64   `json:"loop_watch_age_sec"`
}

// LoopHealth summarizes background goroutines.
type LoopHealth struct {
	RenewAgeSec  float64 `json:"renew_age_sec"`
	HealthAgeSec float64 `json:"health_age_sec"`
	WatchAgeSec  float64 `json:"watch_age_sec"`
}

// Status is returned by /status.
type Status struct {
	Node           string                   `json:"node"`
	Role           string                   `json:"role"`
	Mode           string                   `json:"mode"`
	Leader         bool                     `json:"leader"`
	DeadVotes      int                      `json:"dead_votes"`
	LastDecision   string                   `json:"last_decision"`
	Epoch          uint64                   `json:"epoch"`
	Primary        string                   `json:"primary"`
	Maintenance    bool                     `json:"maintenance"`
	MaintenanceMsg string                   `json:"maintenance_message,omitempty"`
	Loops          LoopHealth               `json:"loops"`
	Nodes          []NodeTopology           `json:"nodes"`
	Guards         []GuardTopology          `json:"guards"`
	ProxySQL       []proxysql.RuntimeServer `json:"proxysql_servers,omitempty"`
	UpdatedAt      time.Time                `json:"updated_at"`
}
