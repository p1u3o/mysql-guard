// Package semistall decides when the guard turns semi-sync source off because
// the replica is dead, and when it turns it on again. It is pure: the runtime
// feeds it one observation per leader tick and acts on the returned decision.
//
// Why: with rpl_semi_sync_source_enabled=ON and AFTER_SYNC, a commit waits for
// a replica acknowledgement for up to rpl_semi_sync_source_timeout (10s in
// prod). When the only replica dies, writers stall until that timeout fires.
// Turning the source switch off releases them at once. It is only done when
// the replica is confirmed dead by independent signals, never on one view:
//
//   - the leader's own probe of the replica failed,
//   - the witness's connect check of the replica says "dead" (a partition
//     between the primary and the replica alone must not count),
//   - the primary reports no semi-sync client (the replica holds no ack stream),
//
// all for Probes consecutive ticks. Turning it on again needs the replica
// reachable, read-only, IO and SQL threads running, IO not lost, not stalled,
// lag within MaxLag, no GTID gap to the writer and replica-side semi-sync
// enabled, for Reenable consecutive ticks. Only a disable that the guard made
// itself (Input.OffByGuard) is ever undone.
package semistall

import (
	"fmt"
	"time"
)

// Action is what the runtime should do this tick.
type Action int

const (
	None     Action = iota
	Disable         // SET GLOBAL rpl_semi_sync_source_enabled=OFF on the primary
	Reenable        // SET GLOBAL rpl_semi_sync_source_enabled=ON on the primary
	Clear           // the switch was turned on by someone else: forget the guard's own disable
)

func (a Action) String() string {
	switch a {
	case Disable:
		return "disable"
	case Reenable:
		return "reenable"
	case Clear:
		return "clear"
	}
	return "none"
}

// Settings are the thresholds (config flags).
type Settings struct {
	Probes   int     // consecutive dead ticks before Disable (min 1)
	Reenable int     // consecutive healthy ticks before Reenable (min 1)
	MaxLag   float64 // seconds the replica may lag at re-enable
}

// Input is the leader's view for one tick.
type Input struct {
	// Blocked is non-empty when the guard must not act at all this tick
	// (maintenance, failover/fence/switchover in flight, not leader, ...).
	// It resets the counters: confirmation must be uninterrupted.
	Blocked string

	PrimaryReachable bool
	PrimaryWritable  bool // reachable and not read-only
	SourceEnabled    bool // rpl_semi_sync_source_enabled on the primary
	SourceClients    int  // Rpl_semi_sync_source_clients on the primary

	ReplicaReachable bool   // leader's own probe of the replica
	WitnessProbe     string // witness connect check of the replica: "dead", "ok", "" (none/stale)

	// Replica health for the re-enable (read from the replica itself).
	ReplKnown      bool
	IOThread       bool
	SQLThread      bool
	IOLost         bool
	ReplicaRO      bool
	Stalled        bool
	LagSeconds     float64
	GapToWriter    int64 // transactions on the primary the replica has not applied
	ReplicaSemiOn  bool  // replica side active: rpl_semi_sync_replica_enabled and Rpl_semi_sync_replica_status ON
	ReplicaInstall bool  // semi-sync plugin present on the replica

	// OffByGuard: the Consul record says the guard turned the source off.
	OffByGuard bool
}

// Decision is the outcome of Step.
type Decision struct {
	Action Action
	Reason string // always set: why this action, or why none
	Skip   string // short metric label when a candidate action was held back ("" = nothing pending)
}

// Tracker counts consecutive ticks.
type Tracker struct {
	dead    int
	healthy int
	// Cooldown: after a failed re-enable no new attempt before this time.
	notBefore time.Time
}

// Reset drops all counters.
func (t *Tracker) Reset() { t.dead, t.healthy = 0, 0 }

// Backoff holds re-enable attempts until at (a failed attempt must not flap).
func (t *Tracker) Backoff(at time.Time) { t.notBefore = at; t.healthy = 0 }

// DeadTicks and HealthyTicks expose the counters (status and tests).
func (t *Tracker) DeadTicks() int    { return t.dead }
func (t *Tracker) HealthyTicks() int { return t.healthy }

func norm(s Settings) Settings {
	if s.Probes < 1 {
		s.Probes = 1
	}
	if s.Reenable < 1 {
		s.Reenable = 1
	}
	return s
}

// Step evaluates one tick.
func (t *Tracker) Step(now time.Time, in Input, s Settings) Decision {
	s = norm(s)
	if in.Blocked != "" {
		t.Reset()
		return Decision{Action: None, Reason: "blocked: " + in.Blocked, Skip: "blocked"}
	}
	if in.OffByGuard {
		return t.stepOff(now, in, s)
	}
	t.healthy = 0
	// Only a primary that is up, writable and has the switch on can stall.
	if !in.PrimaryReachable || !in.PrimaryWritable {
		t.dead = 0
		return Decision{Action: None, Reason: "primary not reachable and writable"}
	}
	if !in.SourceEnabled {
		t.dead = 0
		return Decision{Action: None, Reason: "semi-sync source not enabled"}
	}
	why := deadWhy(in)
	if why != "" {
		t.dead = 0
		return Decision{Action: None, Reason: "replica not confirmed dead: " + why, Skip: "not_confirmed"}
	}
	t.dead++
	if t.dead < s.Probes {
		return Decision{Action: None, Reason: fmt.Sprintf("replica dead %d/%d ticks", t.dead, s.Probes), Skip: "confirming"}
	}
	t.Reset()
	return Decision{Action: Disable, Reason: fmt.Sprintf("replica confirmed dead for %d ticks (own probe failed, witness dead, no semi-sync client)", s.Probes)}
}

// deadWhy returns "" when every dead signal holds, else the first that does not.
func deadWhy(in Input) string {
	switch {
	case in.ReplicaReachable:
		return "leader can reach it"
	case in.WitnessProbe == "ok":
		return "witness can reach it"
	case in.WitnessProbe != "dead":
		return "no fresh witness check"
	case in.SourceClients > 0:
		return fmt.Sprintf("%d semi-sync client(s) still connected", in.SourceClients)
	}
	return ""
}

func (t *Tracker) stepOff(now time.Time, in Input, s Settings) Decision {
	t.dead = 0
	if in.SourceEnabled {
		t.healthy = 0
		return Decision{Action: Clear, Reason: "semi-sync source already on again (changed outside the guard)"}
	}
	if !in.PrimaryReachable || !in.PrimaryWritable {
		t.healthy = 0
		return Decision{Action: None, Reason: "primary not reachable and writable"}
	}
	if why := healthyWhy(in, s); why != "" {
		t.healthy = 0
		return Decision{Action: None, Reason: "semi-sync stays off: replica " + why, Skip: "replica_not_ready"}
	}
	if now.Before(t.notBefore) {
		return Decision{Action: None, Reason: "semi-sync stays off: backing off after a failed re-enable", Skip: "backoff"}
	}
	t.healthy++
	if t.healthy < s.Reenable {
		return Decision{Action: None, Reason: fmt.Sprintf("replica healthy %d/%d ticks", t.healthy, s.Reenable), Skip: "confirming"}
	}
	t.Reset()
	return Decision{Action: Reenable, Reason: fmt.Sprintf("replica back, caught up (lag %.1fs, gap %d) for %d ticks", in.LagSeconds, in.GapToWriter, s.Reenable)}
}

func healthyWhy(in Input, s Settings) string {
	switch {
	case !in.ReplicaReachable:
		return "unreachable"
	case !in.ReplKnown:
		return "replication state unknown"
	case !in.ReplicaRO:
		return "is writable"
	case !in.IOThread || !in.SQLThread:
		return fmt.Sprintf("threads not running (io=%t sql=%t)", in.IOThread, in.SQLThread)
	case in.IOLost:
		return "io thread lost the primary"
	case in.Stalled:
		return "is stalled"
	case in.LagSeconds > s.MaxLag:
		return fmt.Sprintf("lags %.1fs (max %.1fs)", in.LagSeconds, s.MaxLag)
	case in.GapToWriter > 0:
		return fmt.Sprintf("is %d transactions behind the primary", in.GapToWriter)
	case !in.ReplicaInstall:
		return "has no semi-sync plugin"
	case !in.ReplicaSemiOn:
		return "semi-sync replica side not active (rpl_semi_sync_replica_enabled off or IO thread started before it)"
	}
	return ""
}
