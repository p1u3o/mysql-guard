package guard

import "time"

// NodeTopology is one DB peer in /status.
type NodeTopology struct {
	Node      string  `json:"node"`
	Executed  string  `json:"gtid_executed"`
	Retrieved string  `json:"gtid_retrieved"`
	LagSec    float64 `json:"lag_seconds"`
	ReadOnly  bool    `json:"read_only"`
	IOThread  bool    `json:"io_thread"`
	SQLThread bool    `json:"sql_thread"`
}

// GuardTopology is a running guard instance.
type GuardTopology struct {
	Node   string `json:"node"`
	Role   string `json:"role"`
	Leader bool   `json:"leader"`
	Up     bool   `json:"up"`
}

// Status is returned by /status.
type Status struct {
	Node           string          `json:"node"`
	Role           string          `json:"role"`
	Mode           string          `json:"mode"`
	Leader         bool            `json:"leader"`
	DeadVotes      int             `json:"dead_votes"`
	LastDecision   string          `json:"last_decision"`
	Epoch          uint64          `json:"epoch"`
	Primary        string          `json:"primary"`
	Maintenance    bool            `json:"maintenance"`
	MaintenanceMsg string          `json:"maintenance_message,omitempty"`
	Nodes          []NodeTopology  `json:"nodes"`
	Guards         []GuardTopology `json:"guards"`
	UpdatedAt      time.Time       `json:"updated_at"`
}
