package guard

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"mysql-guard/internal/actionlog"
	"mysql-guard/internal/actions"
	"mysql-guard/internal/config"
	"mysql-guard/internal/consul"
	"mysql-guard/internal/decision"
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

	mu          sync.RWMutex
	lastMaint   bool
	ballot      vote.Ballot
	status      Status
	epoch       uint64
	lastFail    time.Time
	lastHealth  time.Time
	lastWatch   time.Time
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
		status: Status{Node: cfg.NodeName, Role: string(cfg.Role), Mode: string(cfg.Mode)},
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

func (r *Runtime) healthTick(ctx context.Context, now time.Time) {
	res, elapsed := r.mysql.Ping(ctx)
	r.metrics.ProbeSeconds.Observe(elapsed.Seconds())
	switch res {
	case vote.ProbeSlow:
		r.metrics.ProbeSlowTotal.Inc()
	case vote.ProbeFailed, vote.ProbeRefused:
		r.metrics.ProbeFailTotal.Inc()
	}

	r.mu.Lock()
	r.ballot.UpdateAll(now, res, vote.ProbeOK, vote.ProbeOK, r.cfg.SlowThreshold, r.cfg.DeadAfter)
	dead := r.ballot.DeadCount()
	ballot := r.ballot
	r.mu.Unlock()

	r.metrics.DeadVotes.WithLabelValues("v1").Set(boolToF(ballot.V1.Dead))
	r.metrics.DeadVotes.WithLabelValues("total").Set(float64(dead))

	maint, maintMsg, _ := r.store.MaintenanceEnabled(ctx)
	primary, epochKV, _ := r.store.GetPrimary(ctx)
	if maint != r.lastMaint {
		r.logMaintenance(ctx, maint, maintMsg, epochKV)
		r.lastMaint = maint
	}
	topology := r.buildTopology(ctx, primary)

	replSt := replhealth.State{Node: r.cfg.NodeName, ExpectReadOnly: r.cfg.NodeName != primary}
	if st, err := r.mysql.ReadGTIDState(ctx); err == nil {
		replSt.ReadOnly = st.ReadOnly
		replSt.IOThreadOn = st.IOThread
		replSt.SQLThreadOn = st.SQLThread
		replSt.LagSeconds = st.LagSeconds
	}
	r.exec.PublishReplicationAlerts(ctx, replSt)

	if r.lock.IsLeader() {
		_ = MaybeSeedMaster(ctx, r.log, r.store, r.mysql, r.cfg.NodeName, r.cfg.Peers, true)
	}
	if r.lock.IsLeader() && r.lock.VerifyLeaderBeforeAction(r.cfg.SessionTTL) {
		r.decide(ctx, now, dead, ballot, maint, epochKV, topology)
	}
	r.lastHealth = now
	r.refreshStatus(ctx, now, dead, maint, maintMsg, primary, epochKV, topology)
}

func (r *Runtime) buildTopology(ctx context.Context, primary string) []NodeTopology {
	var nodes []NodeTopology
	for _, p := range r.cfg.Peers {
		nt := NodeTopology{Node: p}
		if rec, err := r.store.GetGTID(ctx, p); err == nil && rec != nil {
			nt.Executed = rec.Executed
			nt.Retrieved = rec.Retrieved
		}
		var st mysql.GTIDState
		var err error
		if r.peers != nil {
			st, err = r.peers.ReadState(ctx, p)
		} else if p == r.cfg.NodeName {
			st, err = r.mysql.ReadGTIDState(ctx)
		}
		if err == nil {
			if st.Executed != "" {
				nt.Executed = st.Executed
				nt.Retrieved = st.Retrieved
				nt.ReadOnly = st.ReadOnly
				nt.SuperReadOnly = st.SuperReadOnly
				nt.IOThread = st.IOThread
				nt.SQLThread = st.SQLThread
				nt.LagSec = st.LagSeconds
			}
		}
		if p == primary {
			nt.ProxySQLStatus = "writer"
			nt.ProxySQLHG = r.cfg.ProxySQLWriterHG
		} else {
			nt.ProxySQLStatus = "replica"
			nt.ProxySQLHG = r.cfg.ProxySQLReaderHG
		}
		nt.ReadOnly = nt.ReadOnly || p != primary
		nodes = append(nodes, nt)
	}
	return nodes
}

func boolToF(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

func (r *Runtime) decide(ctx context.Context, now time.Time, dead int, ballot vote.Ballot, maint bool, epochKV uint64, topology []NodeTopology) {
	r.mu.RLock()
	epoch := r.epoch
	lastFail := r.lastFail
	r.mu.RUnlock()

	primary, _, _ := r.store.GetPrimary(ctx)
	replica := decision.ReplicaState{Node: r.cfg.NodeName, ReadOnly: true, IOThreadRunning: true, SQLThreadKnown: true}
	local := gtid.Set("")
	primaryExecuted := gtid.Set("")
	if st, err := r.mysql.ReadGTIDState(ctx); err == nil {
		replica.ReadOnly = st.ReadOnly
		replica.IOThreadRunning = st.IOThread
		replica.SQLThreadKnown = st.SQLThread
		replica.Executed = gtid.Parse(st.Executed)
		replica.Retrieved = gtid.Parse(st.Retrieved)
		local = replica.Executed
	}
	peerGTID := map[string]gtid.Set{}
	for _, nt := range topology {
		peerGTID[nt.Node] = gtid.Parse(nt.Executed)
		if nt.Node == primary {
			primaryExecuted = gtid.Parse(nt.Executed)
		}
	}

	in := decision.ScenarioInput{
		Now:          now,
		Ballot:       ballot,
		VotesNeeded:  r.cfg.VotesNeeded,
		ConsulUp:     true,
		Maintenance:  maint,
		IsLeader:     true,
		ReplicaLag:   replica,
		PeerGTID:     peerGTID,
		LocalEpoch:   epoch,
		RemoteEpoch:  epochKV,
		LastFailover: lastFail,
		MinBetween:   r.cfg.MinBetweenFail,
	}
	if !replica.ReadOnly {
		in.PrimaryWritable = true
	}
	sc := decision.EvaluateScenario(in)
	if r.cfg.Mode == config.ModeObserve && sc.Action != "none" {
		r.metrics.WouldActTotal.Inc()
	}
	oldPrimary := primary
	if oldPrimary == "" {
		oldPrimary = r.cfg.Peers[0]
	}
	r.exec.RunScenario(ctx, sc, r.cfg.NodeName, oldPrimary, local, primaryExecuted, epochKV)

	r.mu.Lock()
	r.status.LastDecision = sc.Action + ": " + sc.Reason
	r.mu.Unlock()
}

func (r *Runtime) refreshStatus(ctx context.Context, now time.Time, dead int, maint bool, maintMsg, primary string, epoch uint64, nodes []NodeTopology) {
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
