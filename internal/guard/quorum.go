package guard

import (
	"sort"
	"strings"
	"time"

	"mysql-guard/internal/config"
	"mysql-guard/internal/consul"
)

// Quorum (v0.3.6): who may declare the primary dead.
//
// A failover needs Votes >= -votes-needed (default 2) on deadTicks (3)
// consecutive leader ticks, and then still passes the candidate gates
// (stall, GTID superset, semi-sync, rate limit, epoch CAS).
//
// Voters, at most one vote each, and only from fresh heartbeats (younger than
// VoteMaxAge: 3*interval + 2*probe-timeout + 1s) that are about the same primary as the leader's view:
//
//  1. db guard "probe": the guard's own MySQL probe of the primary failed
//     (PrimaryOK=false). The leader counts its own probe directly.
//  2. witness "probe": the witness connect check of the primary (mg_* login,
//     SELECT 1) failed with a network error or timeout (PrimaryProbe="dead").
//     A MySQL server error (e.g. access denied) proves the host is up and is
//     "ok". A witness without credentials publishes "" and never votes.
//  3. replica "io": a replica's IO thread has lost the primary (connecting,
//     erroring, or no event/heartbeat for -replica-stale) AND the primary
//     node's own guard heartbeat is stale or missing. On its own an IO-thread
//     loss is only a replica-side view (a partition between the replica and
//     the primary looks the same), so it never votes while the primary's
//     guard still reports in, nor while a fresh witness connect check reports
//     the primary "ok". At most one io vote is counted. The io vote may come
//     from the same replica as a probe vote: the two are independent signals
//     (a new connection failing, and the existing replication stream dead)
//     and it is what lets a two-node pair without a probing witness survive
//     the loss of the whole primary host.
//
// Outcomes with two db nodes and one witness:
//   - whole host lost (mysqld + guard): replica probe + witness probe + io
//     (replica probe + io when the witness does not probe)
//   - mysqld down, host up: primary guard probe + replica probe
//   - mysqld down and witness lost: primary guard probe + replica probe
//   - replica isolated (or replica<->primary link down): only the replica
//     votes; the witness and the primary guard see the primary -> no failover
//   - witness isolated: witness probe alone -> no failover
type Quorum struct {
	Votes             int
	Voters            []string // e.g. "kv02:probe", "prod02:witness", "kv02:io"
	PrimaryGuardStale bool
}

func (q Quorum) String() string {
	if len(q.Voters) == 0 {
		return "no votes"
	}
	return strings.Join(q.Voters, ",")
}

// QuorumInput is the leader's view for one tick.
type QuorumInput struct {
	Self          string
	SelfRole      string
	SelfPrimaryOK bool
	SelfIOLost    bool
	Primary       string
	Guards        []consul.GuardInstance
	Now           time.Time
	MaxAge        time.Duration
}

// CountQuorum applies the rules above.
func CountQuorum(in QuorumInput) Quorum {
	var q Quorum
	if in.Primary == "" {
		return q
	}
	witnessOK := false
	ioLost := map[string]bool{}
	if in.SelfRole == "" || in.SelfRole == string(config.RoleDB) {
		if !in.SelfPrimaryOK {
			q.Voters = append(q.Voters, in.Self+":probe")
		}
		if in.SelfIOLost && in.Self != in.Primary {
			ioLost[in.Self] = true
		}
	}
	primaryGuardFresh := false
	if in.Self == in.Primary {
		primaryGuardFresh = true // the leader is the primary's guard
	}
	for _, g := range in.Guards {
		if g.Node == in.Self {
			continue
		}
		fresh := in.Now.Sub(g.LastHeartbeat) <= in.MaxAge
		if !fresh {
			continue
		}
		if g.Node == in.Primary && g.Role == string(config.RoleDB) {
			primaryGuardFresh = true
		}
		if g.PrimaryView != in.Primary {
			continue
		}
		switch g.Role {
		case string(config.RoleDB):
			if !g.PrimaryOK {
				q.Voters = append(q.Voters, g.Node+":probe")
			}
			if g.ReplicaIOLost && g.Node != in.Primary {
				ioLost[g.Node] = true
			}
		case string(config.RoleWitness):
			switch g.PrimaryProbe {
			case "dead":
				q.Voters = append(q.Voters, g.Node+":witness")
			case "ok":
				witnessOK = true
			}
		}
	}
	q.PrimaryGuardStale = !primaryGuardFresh
	if q.PrimaryGuardStale && !witnessOK {
		var nodes []string
		for n := range ioLost {
			nodes = append(nodes, n)
		}
		sort.Strings(nodes)
		if len(nodes) > 0 {
			q.Voters = append(q.Voters, nodes[0]+":io")
		}
	}
	sort.Strings(q.Voters)
	q.Votes = len(q.Voters)
	return q
}

// CountDeadVotes is the pre-v0.3.6 helper kept for callers/tests: db guard
// probe votes only.
func CountDeadVotes(self string, selfPrimaryOK bool, primary string, guards []consul.GuardInstance, now time.Time, maxAge time.Duration) int {
	var dbOnly []consul.GuardInstance
	for _, g := range guards {
		if g.Role == string(config.RoleDB) {
			g.ReplicaIOLost = false
			dbOnly = append(dbOnly, g)
		}
	}
	return CountQuorum(QuorumInput{Self: self, SelfPrimaryOK: selfPrimaryOK, Primary: primary, Guards: dbOnly, Now: now, MaxAge: maxAge}).Votes
}

// VoteMaxAge is how old a guard heartbeat may be and still vote. A guard whose
// primary just died spends up to a probe timeout (or two) per tick waiting on
// it, so its heartbeats arrive slower than -interval exactly when its vote is
// needed; 3*interval alone dropped the primary guard's probe vote (drill D4).
func VoteMaxAge(interval, probeTimeout time.Duration) time.Duration {
	return 3*interval + 2*probeTimeout + time.Second
}
