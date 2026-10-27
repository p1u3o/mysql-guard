package guard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"mysql-guard/internal/stall"
	"strings"
	"sync"
	"time"

	"mysql-guard/internal/actionlog"
	"mysql-guard/internal/actions"
	"mysql-guard/internal/config"
	"mysql-guard/internal/consul"
	"mysql-guard/internal/gtid"
	"mysql-guard/internal/metrics"
	"mysql-guard/internal/mysql"
	"mysql-guard/internal/proxysql"
	"mysql-guard/internal/replhealth"
	"mysql-guard/internal/vote"

	capi "github.com/hashicorp/consul/api"
)

// Runtime coordinates health, leader-watch, and decision ticks.
type Runtime struct {
	cfg     *config.Config
	log     *slog.Logger
	lock    *consul.LeaderLock
	store   *consul.Store
	mysql   *mysql.Prober
	peers   *mysql.Pool
	proxy   *proxysql.Client
	exec    *actions.Executor
	metrics *metrics.Registry

	mu         sync.RWMutex
	lastMaint  bool
	ballot     vote.Ballot
	status     Status
	epoch      uint64
	lastFail   time.Time
	lastHealth time.Time
	lastWatch  time.Time
	writable   map[string]int
	deadTicks  int
	roSince    time.Time // primary first seen reachable but read-only (zero = not read-only)
	health     HealthReporter
	stall      *stall.Tracker
	stalled    map[string]string // node -> reason, this tick
	lastWriter consul.WriterGTID // last-known writer executed set
	lwDirty    bool              // lastWriter not yet published to Consul (retry until a leader write succeeds)

	// v0.3.0
	foreignTicks int
	foreignAlert string
	pxSince      time.Time
	pxAlerted    string
	svcLastErr   string
}

// Version is the build version shown in /status (set from main via ldflags).
var Version = "dev"

// HealthReporter receives the guard's own health each tick (Consul TTL check).
type HealthReporter interface {
	UpdateGuard(status, output string) error
}

// SetHealthReporter wires the guard's own TTL check. Call before Run.
func (r *Runtime) SetHealthReporter(h HealthReporter) { r.health = h }

// tickHealth is what one health tick observed about this guard's own duties.
type tickHealth struct {
	role        config.Role
	probe       vote.ProbeResult // local MySQL (db role only)
	consulErr   error            // writer-key read
	replUnknown bool
	replErr     string
	leader      bool
	primary     string
	epoch       uint64
	stalled     []string
}

// guardCheckStatus maps a tick to the TTL check status (Consul HealthPassing/Warning/Critical).
func guardCheckStatus(t tickHealth) (string, string) {
	if t.consulErr != nil {
		return "critical", "consul kv unreadable: " + t.consulErr.Error()
	}
	if t.role != config.RoleWitness {
		switch t.probe {
		case vote.ProbeFailed, vote.ProbeRefused:
			return "critical", "local mysql unreachable"
		}
		if t.replUnknown {
			return "warning", "replication state unknown: " + t.replErr
		}
		if t.probe == vote.ProbeSlow {
			return "warning", "local mysql slow"
		}
	}
	if len(t.stalled) > 0 {
		return "warning", "replica stalled (never a promotion candidate): " + strings.Join(t.stalled, ",")
	}
	return "passing", fmt.Sprintf("ok role=%s leader=%t primary=%s epoch=%d", t.role, t.leader, t.primary, t.epoch)
}

func (r *Runtime) reportHealth(t tickHealth) {
	if r.health == nil {
		return
	}
	st, out := guardCheckStatus(t)
	_ = r.health.UpdateGuard(st, out)
}

func NewRuntime(cfg *config.Config, log *slog.Logger, store *consul.Store, lock *consul.LeaderLock, my *mysql.Prober, peers *mysql.Pool, px *proxysql.Client, exec *actions.Executor, m *metrics.Registry) *Runtime {
	return &Runtime{
		cfg:     cfg,
		log:     log,
		lock:    lock,
		store:   store,
		mysql:   my,
		peers:   peers,
		proxy:   px,
		exec:    exec,
		metrics: m,
		status:  Status{Node: cfg.NodeName, Role: string(cfg.Role), Mode: string(cfg.Mode)},
	}
}

func (r *Runtime) Snapshot() Status {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.status
}

// Run starts health loop and leader-watch loop (renewal is separate).
func (r *Runtime) Run(ctx context.Context) {
	go r.leaderWatchLoop(ctx)
	go r.healthLoop(ctx)
}

func (r *Runtime) healthLoop(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r.healthTick(ctx, now)
		}
	}
}

// Checks lists which probes a guard role performs.
type Checks struct {
	MySQL       bool // local/peer MySQL probes, topology, seed and failover decisions
	Replication bool // replication health alerts
}

// ChecksFor returns the checks a role runs. Witness guards only take part in
// Consul coordination and never touch MySQL.
func ChecksFor(role config.Role) Checks {
	if role == config.RoleWitness {
		return Checks{}
	}
	return Checks{MySQL: true, Replication: true}
}

type peerObs struct {
	st  mysql.GTIDState
	err error
}

func (r *Runtime) healthTick(ctx context.Context, now time.Time) {
	checks := ChecksFor(r.cfg.Role)
	var ballot vote.Ballot
	dead := 0
	th := tickHealth{role: r.cfg.Role}
	if checks.MySQL {
		res, elapsed := r.mysql.Ping(ctx)
		th.probe = res
		r.metrics.ProbeSeconds.Observe(elapsed.Seconds())
		switch res {
		case vote.ProbeSlow:
			r.metrics.ProbeSlowTotal.Inc()
		case vote.ProbeFailed, vote.ProbeRefused:
			r.metrics.ProbeFailTotal.Inc()
		}
		r.mu.Lock()
		r.ballot.UpdateAll(now, res, vote.ProbeOK, vote.ProbeOK, r.cfg.SlowThreshold, r.cfg.DeadAfter)
		ballot = r.ballot
		r.mu.Unlock()
		r.metrics.DeadVotes.WithLabelValues("v1").Set(boolToF(ballot.V1.Dead))
	}

	maint, maintMsg, _ := r.store.MaintenanceEnabled(ctx)
	primary, epochKV, kvErr := r.store.GetPrimary(ctx)
	th.consulErr = kvErr
	if maint != r.lastMaint {
		r.logMaintenance(ctx, maint, maintMsg, epochKV)
		r.lastMaint = maint
	}

	var topology []NodeTopology
	obs := map[string]peerObs{}
	if checks.MySQL {
		topology, obs = r.buildTopology(ctx, primary)
		r.trackStalls(ctx, now, primary, epochKV, topology, obs)
		th.stalled = r.stalledNodes()
	}
	if checks.Replication {
		replSt := replhealth.State{Node: r.cfg.NodeName, ExpectReadOnly: primary != "" && r.cfg.NodeName != primary}
		if st, err := r.mysql.ReadGTIDState(ctx); err == nil {
			replSt.ReadOnly = st.ReadOnly
			replSt.Unknown = !st.ReplKnown
			replSt.UnknownErr = st.ReplErr
			th.replUnknown, th.replErr = !st.ReplKnown, st.ReplErr
			replSt.IOThreadOn = st.IOThread || !replSt.ExpectReadOnly
			replSt.SQLThreadOn = st.SQLThread || !replSt.ExpectReadOnly
			replSt.LagSeconds = st.LagSeconds
			r.mu.RLock()
			replSt.StallReason, replSt.Stalled = r.stalled[r.cfg.NodeName], r.stalled[r.cfg.NodeName] != ""
			r.mu.RUnlock()
			r.exec.PublishReplicationAlerts(ctx, replSt)
		}
	}

	primaryOK := false
	if checks.MySQL && primary != "" {
		primaryOK = obs[primary].err == nil
	}

	if checks.MySQL && r.lock.VerifyLeaderBeforeAction(r.cfg.SessionTTL) {
		if primary == "" {
			in := SeedInput{IsLeader: true}
			for _, p := range r.cfg.Peers {
				o := obs[p]
				in.Nodes = append(in.Nodes, SeedNode{Node: p, Reachable: o.err == nil, Writable: o.err == nil && !o.st.ReadOnly, Executed: gtid.Parse(o.st.Executed)})
			}
			if r.cfg.Mode == config.ModeObserve {
				if n, why, ok := ShouldSeedMaster(in); ok {
					r.setDecision("observe: would seed writer key " + n + " (" + why + ")")
				}
			} else if node, err := maybeSeedMaster(ctx, r.log, r.store, r.cfg.NodeName, in, r.cfg.ActionLogMax); err != nil {
				r.log.Warn("seed writer key", "err", err)
			} else if node != "" {
				primary, epochKV, _ = r.store.GetPrimary(ctx)
				primaryOK = obs[primary].err == nil
			}
		} else {
			dead = r.decide(ctx, now, maint, primary, epochKV, obs)
		}
	}
	if checks.MySQL && r.lock.IsLeader() {
		po := obs[primary]
		r.v030Checks(ctx, now, maint, primary, epochKV, po.err == nil && po.st.ReadOnly)
	}
	r.metrics.DeadVotes.WithLabelValues("total").Set(float64(dead))
	r.lastHealth = now
	r.refreshStatus(ctx, now, dead, maint, maintMsg, primary, epochKV, topology, primaryOK)
	th.leader, th.primary, th.epoch = r.lock.IsLeader(), primary, epochKV
	r.reportHealth(th)
}

func (r *Runtime) buildTopology(ctx context.Context, primary string) ([]NodeTopology, map[string]peerObs) {
	var nodes []NodeTopology
	obs := map[string]peerObs{}
	for _, p := range r.cfg.Peers {
		nt := NodeTopology{Node: p}
		var st mysql.GTIDState
		var err error
		if r.peers != nil {
			st, err = r.peers.ReadState(ctx, p)
		} else if p == r.cfg.NodeName {
			st, err = r.mysql.ReadGTIDState(ctx)
		} else {
			err = errNoPeerPool
		}
		obs[p] = peerObs{st: st, err: err}
		if err == nil {
			nt.Reachable = true
			nt.Executed = st.Executed
			nt.Retrieved = st.Retrieved
			nt.ReadOnly = st.ReadOnly
			nt.SuperReadOnly = st.SuperReadOnly
			nt.LagSec = st.LagSeconds
			nt.ReplState = st.ReplState()
			nt.ReplError = st.ReplErr
			if st.ReplKnown {
				io, sqlOn := st.IOThread, st.SQLThread
				nt.IOThread, nt.SQLThread = &io, &sqlOn
			}
		} else {
			nt.ReplState = "unknown"
		}
		if r.proxySQLConfigured() {
			if p == primary {
				nt.ProxySQLStatus = "writer"
				nt.ProxySQLHG = r.cfg.ProxySQLWriterHG
			} else {
				nt.ProxySQLStatus = "replica"
				nt.ProxySQLHG = r.cfg.ProxySQLReaderHG
			}
		}
		nodes = append(nodes, nt)
	}
	return nodes, obs
}

var errNoPeerPool = errors.New("peer pool not configured")

// proxySQLConfigured reports whether a ProxySQL admin connection is configured;
// without one, ProxySQL roles are not shown in /status.
func (r *Runtime) proxySQLConfigured() bool {
	return r.proxy != nil && r.proxy.Enabled()
}

// candidateUsable: a failover candidate must be reachable and have a known
// replication state; unknown state is never acted on.
func candidateUsable(o peerObs) bool {
	return o.err == nil && o.st.ReplKnown
}

func boolToF(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// CountDeadVotes counts DB guards that currently see primary as unreachable.
// Only fresh reports about the same primary count; witnesses do not vote.
func CountDeadVotes(self string, selfPrimaryOK bool, primary string, guards []consul.GuardInstance, now time.Time, maxAge time.Duration) int {
	votes := 0
	if !selfPrimaryOK {
		votes++
	}
	for _, g := range guards {
		if g.Node == self || g.Role != string(config.RoleDB) {
			continue
		}
		if g.PrimaryView != primary || now.Sub(g.LastHeartbeat) > maxAge {
			continue
		}
		if !g.PrimaryOK {
			votes++
		}
	}
	return votes
}

// decide runs leader-only fence and failover logic; returns dead vote count.
func (r *Runtime) decide(ctx context.Context, now time.Time, maint bool, primary string, epoch uint64, obs map[string]peerObs) int {
	guards, _ := r.store.ListGuardInstances(ctx)
	dead := CountDeadVotes(r.cfg.NodeName, obs[primary].err == nil, primary, guards, now, 3*r.cfg.Interval+time.Second)
	if maint {
		r.setDecision("none: maintenance mode")
		return dead
	}

	// Fence any non-primary peer that stays writable across two ticks
	// (a planned switchover flips roles within a single tick).
	r.mu.Lock()
	if r.writable == nil {
		r.writable = map[string]int{}
	}
	var toFence []string
	for _, p := range r.cfg.Peers {
		o := obs[p]
		if p != primary && o.err == nil && !o.st.ReadOnly {
			r.writable[p]++
			if r.writable[p] >= 2 {
				toFence = append(toFence, p)
			}
		} else {
			r.writable[p] = 0
		}
	}
	r.mu.Unlock()
	for _, p := range toFence {
		if err := r.exec.FenceNode(ctx, p, epoch, "writable non-primary "+p+" (primary is "+primary+")"); err != nil {
			r.log.Error("fence failed", "node", p, "err", err)
		} else if r.cfg.Mode != config.ModeObserve {
			r.metrics.FencesTotal.Inc()
		}
		r.setDecision("fence: " + p)
	}

	// A reachable primary that stays read-only past ReadOnlyGrace is failed.
	// Complete data (superset of every peer) -> make it writable again
	// (restore, epoch bump, not rate limited). Otherwise fail over.
	roFailed := false
	if po := obs[primary]; po.err == nil && po.st.ReadOnly {
		r.mu.Lock()
		if r.roSince.IsZero() {
			r.roSince = now
		}
		since := r.roSince
		lw := r.lastWriter
		r.mu.Unlock()
		if now.Sub(since) >= r.cfg.ReadOnlyGrace {
			complete, why := r.primaryComplete(primary, po, obs, lw)
			if complete {
				if err := r.exec.RestoreWriter(ctx, primary, epoch, "primary "+primary+" read-only for "+now.Sub(since).Round(time.Second).String()+"; "+why); err != nil {
					r.log.Error("restore writer failed", "node", primary, "err", err)
					r.setDecision("restore_writer failed: " + err.Error())
				} else {
					r.mu.Lock()
					r.roSince = time.Time{}
					r.mu.Unlock()
					r.setDecision("restore_writer: " + primary)
				}
				return dead
			}
			r.log.Warn("read-only primary is missing data; treating as failed", "node", primary, "why", why)
			roFailed = true
		} else {
			r.setDecision("none: primary " + primary + " read-only (grace)")
			return dead
		}
	} else {
		r.mu.Lock()
		r.roSince = time.Time{}
		r.mu.Unlock()
	}

	r.mu.Lock()
	if roFailed {
		r.deadTicks = 3
	} else if dead >= r.cfg.VotesNeeded {
		r.deadTicks++
	} else {
		r.deadTicks = 0
	}
	deadTicks := r.deadTicks
	lastFail := r.lastFail
	r.mu.Unlock()
	if deadTicks < 3 {
		if len(toFence) == 0 {
			r.setDecision("none: primary " + primary + " healthy")
		}
		return dead
	}
	if !lastFail.IsZero() && now.Sub(lastFail) < r.cfg.MinBetweenFail {
		r.setDecision("none: failover rate limited")
		return dead
	}
	cands := map[string]gtid.Set{}
	for _, p := range r.cfg.Peers {
		if o := obs[p]; p != primary && candidateUsable(o) {
			if reason := r.stallReason(p); reason != "" {
				r.exec.LogBlocked(ctx, epoch, "failover", "candidate "+p+" stalled: "+reason, "stalled_candidate")
				continue
			}
			if ok, why := r.supersetOK(ctx, primary, epoch, p, o); !ok {
				r.exec.LogBlocked(ctx, epoch, "failover", why, "superset_gate")
				continue
			}
			cands[p] = gtid.Parse(o.st.Executed)
		}
	}
	if len(cands) == 0 {
		r.setDecision("alert: primary dead, no reachable candidate with known replication state")
		return dead
	}
	cand, ok := gtid.ComparePeers(cands)
	if !ok || cand == "" {
		for c := range cands {
			cand = c
		}
	}
	if r.cfg.Mode == config.ModeObserve {
		r.metrics.WouldActTotal.Inc()
	}
	if err := r.exec.Failover(ctx, primary, cand, epoch); err != nil {
		r.log.Error("failover failed", "err", err)
		r.setDecision("failover failed: " + err.Error())
		return dead
	}
	if r.cfg.Mode == config.ModeAct {
		r.metrics.FailoversTotal.Inc()
	}
	r.mu.Lock()
	r.lastFail = now
	r.deadTicks = 0
	r.mu.Unlock()
	r.setDecision("failover: " + primary + " -> " + cand)
	return dead
}

// primaryComplete reports whether the primary's executed set includes every
// reachable peer's executed set and the last published writer set (used for
// unreachable peers). Unknown state counts as incomplete.
func (r *Runtime) primaryComplete(primary string, po peerObs, obs map[string]peerObs, lw consul.WriterGTID) (bool, string) {
	own := gtid.Parse(po.st.Executed)
	for _, p := range r.cfg.Peers {
		if p == primary {
			continue
		}
		o := obs[p]
		if o.err != nil {
			if lw.Executed == "" {
				return false, "peer " + p + " unreachable and no last-writer GTID set known"
			}
			if !own.Superset(gtid.Parse(lw.Executed)) {
				return false, "missing last-writer GTIDs " + string(gtid.Subtract(gtid.Parse(lw.Executed), own))
			}
			continue
		}
		if !own.Superset(gtid.Parse(o.st.Executed)) {
			return false, "missing GTIDs from " + p + ": " + string(gtid.Subtract(gtid.Parse(o.st.Executed), own))
		}
	}
	return true, "GTIDs include every peer's"
}

func (r *Runtime) setDecision(d string) {
	r.mu.Lock()
	r.status.LastDecision = d
	r.mu.Unlock()
}

func (r *Runtime) refreshStatus(ctx context.Context, now time.Time, dead int, maint bool, maintMsg, primary string, epoch uint64, nodes []NodeTopology, primaryOK bool) {
	if r.cfg.Role == config.RoleWitness {
		// Witnesses hold no session; their liveness is this loop publishing the heartbeat.
		r.lock.MarkAlive(now)
	}
	leader := r.lock.IsLeader()
	renewAge := time.Since(r.lock.LastRenew()).Seconds()
	r.metrics.Leader.Set(boolToF(leader))
	r.metrics.SessionRenewAge.Set(renewAge)
	r.metrics.Epoch.Set(float64(epoch))

	healthAge := 0.0
	if !r.lastHealth.IsZero() {
		healthAge = now.Sub(r.lastHealth).Seconds()
	}
	watchAge := 0.0
	if !r.lastWatch.IsZero() {
		watchAge = now.Sub(r.lastWatch).Seconds()
	}

	inst := consul.GuardInstance{
		Node: r.cfg.NodeName, Role: string(r.cfg.Role), Mode: string(r.cfg.Mode),
		Leader: leader, LastHeartbeat: now,
		LoopRenewSec: renewAge, LoopHealthSec: healthAge, LoopWatchSec: watchAge,
		PrimaryView: primary, PrimaryOK: primaryOK,
	}
	_ = r.store.PutGuardInstance(ctx, inst)

	guards := []GuardTopology{{
		Node: r.cfg.NodeName, Role: string(r.cfg.Role), Mode: string(r.cfg.Mode),
		Leader: leader, Follower: !leader, Up: true, LastHeartbeat: now,
		LoopRenewSec: renewAge, LoopHealthSec: healthAge, LoopWatchSec: watchAge,
	}}
	if list, err := r.store.ListGuardInstances(ctx); err == nil && len(list) > 0 {
		guards = make([]GuardTopology, 0, len(list))
		for _, g := range list {
			guards = append(guards, GuardTopology{
				Node: g.Node, Role: g.Role, Mode: g.Mode, Leader: g.Leader, Follower: !g.Leader, Up: true,
				LastHeartbeat: g.LastHeartbeat, LoopRenewSec: g.LoopRenewSec,
				LoopHealthSec: g.LoopHealthSec, LoopWatchSec: g.LoopWatchSec,
			})
		}
	}

	r.mu.Lock()
	r.status.Leader = leader
	r.status.Version = Version
	r.status.DeadVotes = dead
	r.status.Primary = primary
	r.status.Epoch = epoch
	r.status.Maintenance = maint
	r.status.MaintenanceMsg = maintMsg
	r.status.Nodes = nodes
	r.status.Guards = guards
	r.status.Loops = LoopHealth{RenewAgeSec: renewAge, HealthAgeSec: healthAge, WatchAgeSec: watchAge}
	if r.proxy != nil {
		if rows, err := r.proxy.ListRuntimeServers(ctx); err == nil {
			r.status.ProxySQL = rows
		}
	}
	r.status.UpdatedAt = now
	r.mu.Unlock()
}

func (r *Runtime) logMaintenance(ctx context.Context, on bool, msg string, epoch uint64) {
	action := "maintenance_off"
	reason := "maintenance cleared"
	if on {
		action = "maintenance_on"
		reason = msg
		if reason == "" {
			reason = "maintenance enabled"
		}
	}
	_ = r.store.AppendActionLog(ctx, actionlog.Entry{
		Timestamp: nowUTC(),
		Epoch:     epoch,
		Guard:     r.cfg.NodeName,
		Action:    action,
		Reason:    reason,
		Result:    "ok",
		Step:      "maintenance",
	}, r.cfg.ActionLogMax)
}

func nowUTC() time.Time { return time.Now().UTC() }

func (r *Runtime) leaderWatchLoop(ctx context.Context) {
	client := r.store.Client()
	key := r.store.Keys().LeaderLock()
	var waitIndex uint64
	for {
		if ctx.Err() != nil {
			return
		}
		opts := &capi.QueryOptions{WaitIndex: waitIndex, WaitTime: 30 * time.Second}
		pair, meta, err := client.KV().Get(key, opts)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		if meta != nil {
			waitIndex = meta.LastIndex
		}
		r.mu.Lock()
		r.lastWatch = time.Now()
		r.mu.Unlock()
		if pair != nil && pair.Session != "" {
			r.log.Debug("leader lock changed", "session", pair.Session)
		}
	}
}

// trackStalls updates the stall tracker for every replica and publishes the
// last-known writer's executed set (in memory and in Consul when leader).
func (r *Runtime) trackStalls(ctx context.Context, now time.Time, primary string, epoch uint64, topo []NodeTopology, obs map[string]peerObs) {
	r.mu.Lock()
	if r.stall == nil {
		after := r.cfg.StallAfter
		if after <= 0 {
			after = 30 * time.Second
		}
		r.stall = stall.New(after)
	}
	tr := r.stall
	r.mu.Unlock()
	po, srcKnown := obs[primary]
	srcKnown = srcKnown && primary != "" && po.err == nil
	src := gtid.Set("")
	if srcKnown {
		src = gtid.Parse(po.st.Executed)
	}
	stalled := map[string]string{}
	for i := range topo {
		p := topo[i].Node
		if p == primary {
			continue
		}
		o := obs[p]
		in := stall.Obs{Node: p, SourceKnown: srcKnown && o.err == nil && o.st.ReplKnown, SourceExecuted: src,
			Executed: gtid.Parse(o.st.Executed), Retrieved: gtid.Parse(o.st.Retrieved), ThreadsOn: o.st.IOThread && o.st.SQLThread}
		res := tr.Observe(now, in)
		topo[i].Stalled, topo[i].StallReason, topo[i].GapToWriter = res.Stalled, res.Reason, res.Gap
		if res.Stalled {
			stalled[p] = res.Reason
		}
	}
	r.mu.Lock()
	r.stalled = stalled
	if srcKnown && !po.st.ReadOnly && (r.lastWriter.Node != primary || r.lastWriter.Epoch != epoch || string(src) != r.lastWriter.Executed) {
		r.lastWriter = consul.WriterGTID{Node: primary, Epoch: epoch, Executed: string(src), UpdatedAt: now}
		r.lwDirty = true
	}
	lw, dirty := r.lastWriter, r.lwDirty
	r.mu.Unlock()
	// v0.2.7: publish whenever unpublished, not only on the tick that saw the change (the first change
	// usually happens before this guard wins leadership, so the key was never written).
	if dirty && lw.Node != "" && r.lock.IsLeader() && r.cfg.Mode != config.ModeObserve {
		if err := r.store.PutWriterGTID(ctx, lw); err != nil {
			r.log.Warn("publish last writer gtid", "err", err)
			return
		}
		r.mu.Lock()
		if r.lastWriter == lw {
			r.lwDirty = false
		}
		r.mu.Unlock()
	}
}

func (r *Runtime) stalledNodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []string
	for _, p := range r.cfg.Peers {
		if r.stalled[p] != "" {
			out = append(out, p)
		}
	}
	return out
}

func (r *Runtime) stallReason(node string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stalled[node]
}

// supersetOK refuses a candidate whose executed+retrieved set is not a superset
// of the last-known writer's executed set (same writer and epoch only).
func (r *Runtime) supersetOK(ctx context.Context, primary string, epoch uint64, cand string, o peerObs) (bool, string) {
	r.mu.RLock()
	lw := r.lastWriter
	r.mu.RUnlock()
	if lw.Node != primary || lw.Epoch != epoch {
		if rec, err := r.store.GetWriterGTID(ctx); err == nil && rec != nil {
			lw = *rec
		}
	}
	return supersetCheck(lw, primary, epoch, cand, gtid.Parse(o.st.Executed), gtid.Parse(o.st.Retrieved))
}

func supersetCheck(lw consul.WriterGTID, primary string, epoch uint64, cand string, executed, retrieved gtid.Set) (bool, string) {
	if lw.Node != primary || lw.Epoch != epoch || lw.Executed == "" {
		return true, ""
	}
	have := gtid.Union(executed, retrieved)
	missing := gtid.Subtract(gtid.Set(lw.Executed), have)
	if missing.Empty() {
		return true, ""
	}
	return false, fmt.Sprintf("candidate %s not a superset of last-known writer %s epoch %d: missing %s", cand, lw.Node, lw.Epoch, missing)
}
