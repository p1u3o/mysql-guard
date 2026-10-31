package guard

import (
	"context"
	"fmt"
	"time"

	"mysql-guard/internal/config"
	"mysql-guard/internal/consul"
	"mysql-guard/internal/gtid"
	"mysql-guard/internal/semistall"
)

// ---- semi-sync stall handling (-semisync-stall) ----
//
// See internal/semistall for the rules. This is the glue: it builds the
// observation from the leader's tick, asks the tracker, and applies the
// decision through the executor (which re-checks maintenance, transitions and
// the writer epoch right before touching MySQL).

// semiStallBackoff holds a re-enable after one that registered no semi-sync client.
const semiStallBackoff = time.Minute

// ReplicaWitnessView folds the witness connect checks of replica into one
// value: "dead" when every fresh witness (about the same primary) says dead,
// "ok" when any says ok, "" when there is none.
func ReplicaWitnessView(guards []consul.GuardInstance, primary, replica string, now time.Time, maxAge time.Duration) string {
	res := ""
	for _, g := range guards {
		if g.Role != string(config.RoleWitness) || now.Sub(g.LastHeartbeat) > maxAge || g.PrimaryView != primary {
			continue
		}
		switch g.PeerProbes[replica] {
		case "ok":
			return "ok"
		case "dead":
			res = "dead"
		}
	}
	return res
}

// semiStallBlocked is why the guard must not touch semi-sync this tick ("" = free to act).
func (r *Runtime) semiStallBlocked(ctx context.Context, now time.Time, maint bool, primary string, epoch uint64) string {
	switch {
	case maint:
		return "maintenance mode"
	case r.exec.TransitionHeld(ctx):
		return "failover, fence or switchover in flight"
	case r.gateHolding(primary, epoch):
		return "failover blocked by the semi-sync gate"
	case r.recoveryHolding(now):
		return "primary recovering"
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	switch {
	case r.deadTicks > 0:
		return "primary failover vote in progress"
	case !r.roSince.IsZero():
		return "primary read-only"
	}
	return ""
}

func (r *Runtime) semiStallTick(ctx context.Context, now time.Time, maint bool, primary string, epoch uint64, obs map[string]peerObs) {
	if !r.cfg.SemiSyncStall || primary == "" {
		return
	}
	replica := ""
	for _, p := range r.cfg.Peers {
		if p == primary {
			continue
		}
		if replica != "" {
			return // the stall handler is for a primary with exactly one replica
		}
		replica = p
	}
	if replica == "" {
		return
	}
	po, ro := obs[primary], obs[replica]
	rec, _ := r.store.GetSemiSync(ctx)
	offByGuard := rec != nil && rec.StallDisabled && rec.Node == primary && rec.Epoch == epoch
	r.metrics.SemiStallOff.Set(boolToF(offByGuard))

	guards, _ := r.store.ListGuardInstances(ctx)
	in := semistall.Input{
		Blocked:          r.semiStallBlocked(ctx, now, maint, primary, epoch),
		PrimaryReachable: po.err == nil,
		PrimaryWritable:  po.err == nil && !po.st.ReadOnly,
		SourceEnabled:    po.st.SemiSync.Known && po.st.SemiSync.SourceEnabled,
		SourceClients:    po.st.SemiSync.SourceClients,
		ReplicaReachable: ro.err == nil,
		WitnessProbe:     ReplicaWitnessView(guards, primary, replica, now, VoteMaxAge(r.cfg.Interval, r.cfg.ProbeTimeout)),
		OffByGuard:       offByGuard,
	}
	if !po.st.SemiSync.Known && po.err == nil {
		in.Blocked = "primary semi-sync state unknown"
	}
	if ro.err == nil {
		lost, _ := ro.st.IO.LostSource(r.cfg.ReplicaStale)
		r.mu.RLock()
		stalled := r.stalled[replica] != ""
		r.mu.RUnlock()
		in.ReplKnown, in.IOThread, in.SQLThread = ro.st.ReplKnown, ro.st.IOThread, ro.st.SQLThread
		in.IOLost, in.ReplicaRO, in.Stalled, in.LagSeconds = lost, ro.st.ReadOnly, stalled, ro.st.LagSeconds
		in.GapToWriter = gtid.Subtract(gtid.Parse(po.st.Executed), gtid.Parse(ro.st.Executed)).Count()
		in.ReplicaInstall = ro.st.SemiSync.Known && ro.st.SemiSync.Installed
		in.ReplicaSemiOn = ro.st.SemiSync.ReplicaEnabled && ro.st.SemiSync.ReplicaOn
	}

	d := r.semiStall.Step(now, in, semistall.Settings{Probes: r.cfg.SemiSyncStallProbes, Reenable: r.cfg.SemiSyncReenable, MaxLag: r.cfg.SemiSyncMaxLag})
	r.logSemiStall(d, primary, replica)
	if d.Action == semistall.None {
		if d.Skip != "" {
			r.metrics.SemiStallSkipped.WithLabelValues(d.Skip).Inc()
		}
		return
	}
	if r.cfg.Mode != config.ModeAct {
		r.metrics.WouldActTotal.Inc()
		r.log.Info("semi-sync stall: would act (not in act mode)", "mode", r.cfg.Mode, "action", d.Action.String(), "reason", d.Reason)
		r.metrics.SemiStallSkipped.WithLabelValues("mode_" + string(r.cfg.Mode)).Inc()
		return
	}
	if !r.lock.VerifyLeaderBeforeAction(r.cfg.SessionTTL) {
		r.metrics.SemiStallSkipped.WithLabelValues("not_leader").Inc()
		return
	}
	switch d.Action {
	case semistall.Disable:
		if err := r.exec.SemiStallDisable(ctx, primary, epoch, d.Reason); err != nil {
			r.log.Warn("semi-sync stall: disable failed", "err", err)
			r.metrics.SemiStallSkipped.WithLabelValues("disable_failed").Inc()
			return
		}
		r.metrics.SemiStallDisabled.Inc()
		r.metrics.SemiStallOff.Set(1)
	case semistall.Reenable:
		if err := r.exec.SemiStallReenable(ctx, primary, epoch, d.Reason); err != nil {
			r.log.Warn("semi-sync stall: re-enable failed", "err", err)
			r.semiStall.Backoff(now.Add(semiStallBackoff))
			r.metrics.SemiStallSkipped.WithLabelValues("reenable_failed").Inc()
			return
		}
		r.metrics.SemiStallReenabled.Inc()
		r.metrics.SemiStallOff.Set(0)
	case semistall.Clear:
		r.exec.SemiStallClear(ctx, primary, epoch)
		r.metrics.SemiStallOff.Set(0)
	}
}

// logSemiStall logs a decision when it changes (steady "nothing to do" is silent).
func (r *Runtime) logSemiStall(d semistall.Decision, primary, replica string) {
	key := d.Action.String() + "|" + d.Reason
	if key == r.semiStallLast {
		return
	}
	r.semiStallLast = key
	if d.Action == semistall.None && d.Skip == "" {
		r.log.Debug("semi-sync stall", "primary", primary, "replica", replica, "decision", d.Reason)
		return
	}
	r.log.Info("semi-sync stall", "primary", primary, "replica", replica, "action", d.Action.String(), "decision", fmt.Sprint(d.Reason))
}
