package guard

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"mysql-guard/internal/actions"
	"mysql-guard/internal/config"
	"mysql-guard/internal/consul"
	"mysql-guard/internal/mysql"

	capi "github.com/hashicorp/consul/api"
)

// ---- semi-sync (v0.3.6) ----

type semiSample struct {
	node  string
	epoch uint64
	ss    mysql.SemiSync
	at    time.Time
}

// sampleSemiSync records the primary's semi-sync state each tick it is
// reachable; the leader persists changes to mysql/<cluster>/guard/semisync.
func (r *Runtime) sampleSemiSync(ctx context.Context, now time.Time, primary string, epoch uint64, po peerObs) {
	if po.err != nil || !po.st.SemiSync.Known {
		return
	}
	r.mu.Lock()
	prev := r.semi
	r.semi = semiSample{node: primary, epoch: epoch, ss: po.st.SemiSync, at: now}
	r.mu.Unlock()
	if r.cfg.Mode == config.ModeObserve || !r.lock.IsLeader() {
		return
	}
	if prev.node == primary && prev.epoch == epoch && prev.ss.Mode() == po.st.SemiSync.Mode() && prev.ss.SourceEnabled == po.st.SemiSync.SourceEnabled {
		return
	}
	rec, _ := r.store.GetSemiSync(ctx)
	nr := consul.SemiSyncRecord{Enabled: po.st.SemiSync.SourceEnabled, Mode: po.st.SemiSync.Mode(), Node: primary, Epoch: epoch, At: now.UTC()}
	if rec != nil && rec.Pending && !po.st.SemiSync.SourceEnabled {
		nr.Enabled, nr.Pending = true, true // restore still owed at rejoin
	}
	if err := r.store.PutSemiSync(ctx, nr); err != nil {
		r.log.Warn("semisync kv write", "err", err)
	}
}

// SemiSyncGateDecision is the pure gate: given the last sample of the primary
// (or the Consul record when this guard has none), may an automatic failover
// proceed? Blocks only when semi-sync was enabled on the primary and the last
// state seen was not sync (acknowledged writes may exist only on the primary).
func SemiSyncGateDecision(haveSample bool, enabled bool, mode string, age time.Duration) (bool, string) {
	if !haveSample {
		return true, actions.SemiSyncNote("unknown", 0)
	}
	note := actions.SemiSyncNote(mode, age)
	if !enabled {
		return true, note
	}
	if mode != "sync" {
		return false, note
	}
	return true, note
}

func (r *Runtime) semiSyncGate(ctx context.Context, primary string, epoch uint64) (string, bool) {
	r.mu.RLock()
	s := r.semi
	r.mu.RUnlock()
	have := s.node == primary && s.epoch == epoch && !s.at.IsZero()
	enabled, mode, age := s.ss.SourceEnabled, s.ss.Mode(), time.Since(s.at)
	if !have {
		if rec, _ := r.store.GetSemiSync(ctx); rec != nil && rec.Node == primary && rec.Epoch == epoch {
			have, enabled, mode, age = true, rec.Enabled && !rec.Pending, rec.Mode, time.Since(rec.At)
		}
	}
	ok, note := SemiSyncGateDecision(have, enabled, mode, age)
	if ok || !r.cfg.SemiSyncGate {
		return note, true
	}
	key := fmt.Sprintf("%s|%d", primary, epoch)
	msg := "automatic failover of " + primary + " blocked: semi-sync was enabled but the primary was last seen async, so acknowledged writes may exist only on it (" + note + "); fail over manually after checking"
	r.exec.LogBlocked(ctx, epoch, "failover", msg, "semisync_gate")
	r.mu.Lock()
	first := r.semiBlocked != key
	r.semiBlocked = key
	r.semiBlockAt = time.Now()
	r.mu.Unlock()
	if first {
		r.exec.Alert(ctx, "failover_blocked", msg, true)
	}
	r.setDecision("blocked: semi-sync gate (" + note + ")")
	return note, false
}

// gateHolding: while the semi-sync gate is blocking the failover of primary,
// the replica-side symptoms of that dead primary (repl_* and
// proxysql_writer_mismatch) are the same event as failover_blocked and are
// held, so the event sends exactly one alert.
func (r *Runtime) gateHolding(primary string, epoch uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.semiBlocked == fmt.Sprintf("%s|%d", primary, epoch) &&
		time.Since(r.semiBlockAt) <= VoteMaxAge(r.cfg.Interval, r.cfg.ProbeTimeout)
}

// ---- witness connect check (v0.3.6) ----

func (r *Runtime) witnessProbe(ctx context.Context, primary string) {
	res := ""
	if r.cfg.WitnessProbe && r.peers != nil && primary != "" {
		if _, err := r.peers.DB(primary); err == nil {
			alive, perr := r.peers.Ping(ctx, primary, r.cfg.ProbeTimeout)
			if alive {
				res = "ok"
			} else {
				res = "dead"
			}
			if perr != nil && alive {
				r.log.Debug("witness probe: server answered with error (alive)", "err", perr)
			}
		}
	}
	r.mu.Lock()
	prev := r.primaryProbe
	r.primaryProbe = res
	r.mu.Unlock()
	if prev != res {
		r.log.Info("witness primary probe", "primary", primary, "result", res)
	}
}

// ---- member service (v0.3.6) ----

// MemberInput is one node as the leader sees it this tick.
type MemberInput struct {
	Node         string
	Reachable    bool
	ReplKnown    bool
	ReadOnly     bool
	IOThread     bool
	SQLThread    bool
	IOLost       bool
	LagSeconds   float64
	Stalled      string
	NeedsRebuild bool
}

// MemberDesired maps a node to its member-service role and check status.
// immediate=true for states that must not wait for the debounce (a node that
// is unreachable, writable while not primary, or marked needs-rebuild must not
// keep serving reads).
func MemberDesired(primary string, in MemberInput, maxLag float64) (consul.MemberState, bool) {
	if in.Node == primary {
		switch {
		case !in.Reachable:
			return consul.MemberState{Role: consul.MemberWriter, Status: capi.HealthCritical, Reason: "mysql unreachable"}, false
		case in.ReadOnly:
			return consul.MemberState{Role: consul.MemberWriter, Status: capi.HealthCritical, Reason: "writer is read-only"}, false
		}
		return consul.MemberState{Role: consul.MemberWriter, Status: capi.HealthPassing, Reason: "writable"}, false
	}
	fenced := func(why string, now bool) (consul.MemberState, bool) {
		return consul.MemberState{Role: consul.MemberFenced, Status: capi.HealthCritical, Reason: why}, now
	}
	switch {
	case in.NeedsRebuild:
		return fenced("needs rebuild", true)
	case !in.Reachable:
		return fenced("mysql unreachable", true)
	case !in.ReadOnly:
		return fenced("writable non-primary (fenced)", true)
	case !in.ReplKnown:
		return fenced("replication state unknown", false)
	case !in.IOThread || !in.SQLThread:
		return fenced(fmt.Sprintf("replication stopped (io=%t sql=%t)", in.IOThread, in.SQLThread), false)
	case in.IOLost:
		return fenced("io thread lost the primary", false)
	case in.Stalled != "":
		return fenced("stalled: "+in.Stalled, false)
	case maxLag > 0 && in.LagSeconds > maxLag:
		return consul.MemberState{Role: consul.MemberLagging, Status: capi.HealthWarning, Reason: fmt.Sprintf("lag %.0fs over %.0fs", in.LagSeconds, maxLag)}, false
	}
	return consul.MemberState{Role: consul.MemberReader, Status: capi.HealthPassing, Reason: fmt.Sprintf("replicating, lag %.0fs", in.LagSeconds)}, false
}

// memberDebounce holds applied and pending states per node.
type memberDebounce struct {
	epoch   uint64
	applied map[string]consul.MemberState
	pending map[string]consul.MemberState
	since   map[string]time.Time
}

// Step returns the states to write this tick. A changed desired state is
// applied once it persisted for delay, or at once when immediate. On a new
// epoch the baseline is what SetMaster wrote (new primary writer, others
// fenced) so the next ticks move members back to reader after the delay.
func (d *memberDebounce) Step(now time.Time, epoch uint64, primary string, desired map[string]consul.MemberState, immediate map[string]bool, delay time.Duration) map[string]consul.MemberState {
	if d.applied == nil || d.epoch != epoch {
		d.epoch = epoch
		d.applied, d.pending, d.since = map[string]consul.MemberState{}, map[string]consul.MemberState{}, map[string]time.Time{}
		for n := range desired {
			if n == primary {
				d.applied[n] = consul.MemberState{Role: consul.MemberWriter, Status: capi.HealthPassing, Reason: "promoted"}
			} else {
				d.applied[n] = consul.MemberState{Role: consul.MemberFenced, Status: capi.HealthCritical, Reason: "new epoch: waiting for replication from " + primary}
			}
		}
	}
	for n, want := range desired {
		cur, ok := d.applied[n]
		if ok && cur.Role == want.Role && cur.Status == want.Status {
			d.applied[n] = want // reason refresh
			delete(d.pending, n)
			continue
		}
		if !ok || immediate[n] {
			d.applied[n] = want
			delete(d.pending, n)
			continue
		}
		p, pok := d.pending[n]
		if !pok || p.Role != want.Role || p.Status != want.Status {
			d.pending[n], d.since[n] = want, now
			continue
		}
		d.pending[n] = want
		if now.Sub(d.since[n]) >= delay {
			d.applied[n] = want
			delete(d.pending, n)
		}
	}
	out := map[string]consul.MemberState{}
	for n, s := range d.applied {
		out[n] = s
	}
	return out
}

// syncMembers runs on the leader in act mode outside maintenance.
func (r *Runtime) syncMembers(ctx context.Context, now time.Time, primary string, epoch uint64, obs map[string]peerObs) {
	if r.cfg.MemberServiceName == "" && r.cfg.WriterServiceName == "" {
		return
	}
	guards, _ := r.store.ListGuardInstances(ctx)
	ioLost := map[string]bool{}
	for _, g := range guards {
		if now.Sub(g.LastHeartbeat) <= 3*r.cfg.Interval+time.Second {
			ioLost[g.Node] = g.ReplicaIOLost
		}
	}
	r.mu.RLock()
	ioLost[r.cfg.NodeName] = r.ioLost
	stalled := map[string]string{}
	for k, v := range r.stalled {
		stalled[k] = v
	}
	r.mu.RUnlock()
	desired := map[string]consul.MemberState{}
	immediate := map[string]bool{}
	for _, p := range r.cfg.Peers {
		o := obs[p]
		in := MemberInput{Node: p, Reachable: o.err == nil, ReplKnown: o.st.ReplKnown, ReadOnly: o.st.ReadOnly,
			IOThread: o.st.IOThread, SQLThread: o.st.SQLThread, IOLost: ioLost[p], LagSeconds: o.st.LagSeconds,
			Stalled: stalled[p]}
		if p != primary {
			in.NeedsRebuild = r.store.NeedsRebuildMarked(ctx, p)
		}
		desired[p], immediate[p] = MemberDesired(primary, in, r.cfg.MemberMaxLag)
	}
	r.mu.Lock()
	if r.members == nil {
		r.members = &memberDebounce{}
	}
	states := r.members.Step(now, epoch, primary, desired, immediate, r.cfg.MemberCheckDelay)
	r.mu.Unlock()
	by := r.cfg.NodeName
	if r.cfg.MemberServiceName != "" {
		changed, err := r.store.SyncMembers(ctx, epoch, states, by)
		if err != nil {
			r.svcAlertOnce(ctx, "member_service_sync_failed", r.cfg.MemberServiceName+": "+err.Error())
		}
		if len(changed) > 0 {
			sort.Strings(changed)
			r.log.Info("member service synced", "service", r.cfg.MemberServiceName, "epoch", epoch, "changed", strings.Join(changed, ","))
			r.exec.LogAction(ctx, epoch, "member_service", strings.Join(changed, ","), "ok", "sync")
		}
	}
	if r.cfg.WriterServiceName != "" {
		w := states[primary]
		if err := r.store.SyncWriterCheck(ctx, primary, epoch, w.Status, w.Reason, by); err != nil {
			r.log.Warn("writer check", "err", err)
		}
	}
}

func (r *Runtime) svcAlertOnce(ctx context.Context, code, msg string) {
	r.mu.Lock()
	first := r.memberErr != msg
	r.memberErr = msg
	r.mu.Unlock()
	if first {
		r.exec.Alert(ctx, code, msg, true)
	}
}
