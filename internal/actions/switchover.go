package actions

import (
	"context"
	"fmt"
	"time"

	"mysql-guard/internal/gtid"
	"mysql-guard/internal/mysql"
)

// Switchover runs planned handover using remote peer MySQL connections.
// After the epoch bump, failures use abortSwitchoverPostEpoch (no full rollback).
func (e *Executor) Switchover(ctx context.Context, newPrimaryArg string, local, peer gtid.Set) error {
	if on, msg := e.maintenance(ctx); on {
		e.actionLog(ctx, 0, "switchover", msg, "blocked", "maintenance")
		return fmt.Errorf("maintenance mode")
	}
	oldPrimary, newPrimary := mysql.ResolveOldNewPrimary(e.cfg, newPrimaryArg)
	// v0.3.1: no GTID comparison before the source is fenced. local/peer were
	// read one after the other while the source still took writes, so a replica
	// applying a newer source write between the two reads looked "errant".
	_, _ = local, peer
	if e.observe {
		e.actionLog(ctx, 0, "switchover", "observe mode", "skipped", "end")
		return fmt.Errorf("switchover refused: guard is in observe mode (writes nothing)")
	}
	if e.peers == nil {
		return fmt.Errorf("peer pool not configured")
	}
	oldAdm, err := e.peers.Admin(oldPrimary)
	if err != nil {
		return err
	}
	newAdm, err := e.peers.Admin(newPrimary)
	if err != nil {
		return err
	}
	e.actionLog(ctx, 0, "switchover", "starting", "ok", "begin")
	defer e.markTransition(ctx, "switchover", oldPrimary, newPrimary)()
	rolled := false
	rollback := func() {
		if rolled {
			return
		}
		rolled = true
		_ = oldAdm.SetSuperReadOnly(ctx, false)
		_ = e.proxy.ApplyPromote(ctx, oldPrimary, newPrimary)
		e.actionLog(ctx, 0, "switchover", "pre-epoch rollback: "+oldPrimary+" writable again, epoch unchanged", "ok", "rollback")
	}
	abort := func(step string, cause error) error {
		e.actionLog(ctx, 0, "switchover", cause.Error(), "error", step)
		rollback()
		e.Alert(ctx, "switchover-aborted", fmt.Sprintf("switchover %s -> %s aborted at %s (%s); %s writable again, epoch unchanged", oldPrimary, newPrimary, step, cause, oldPrimary), true)
		return fmt.Errorf("switchover aborted at %s: %w", step, cause)
	}
	// 1. fence the source: super_read_only waits for in-progress commits; then
	// drain (or kill) transactions still open so the final GTID set is stable.
	if err := oldAdm.SetSuperReadOnly(ctx, true); err != nil {
		e.actionLog(ctx, 0, "switchover", err.Error(), "error", "old_super_read_only")
		rollback()
		return err
	}
	e.actionLog(ctx, 0, "switchover", "old primary read-only", "ok", "old_super_read_only")
	if killed, err := oldAdm.DrainTransactions(ctx, e.cfg.FenceDrainTimeout); err != nil {
		return abort("drain_transactions", err)
	} else if killed > 0 {
		e.actionLog(ctx, 0, "switchover", fmt.Sprintf("killed %d open transaction(s) on %s after %s", killed, oldPrimary, e.cfg.FenceDrainTimeout), "warn", "drain_transactions")
	}
	if err := e.proxy.ApplyPromote(ctx, newPrimary, oldPrimary); err != nil {
		return abort("proxysql_offline_old", err)
	}
	// 2. the source's final GTID set, read only after the fence.
	finalSet, err := oldAdm.GTIDExecuted(ctx)
	if err != nil {
		return abort("read_old_gtid", err)
	}
	// 3. target catches up within the timeout, else clean abort (same epoch).
	if err := newAdm.WaitExecuted(ctx, finalSet, e.cfg.CatchupTimeout()); err != nil {
		return abort("catchup_timeout", err)
	}
	// 4. only now the errant check: target minus source must be empty.
	targetSet, err := newAdm.GTIDExecuted(ctx)
	if err != nil {
		return abort("read_new_gtid", err)
	}
	if err := SwitchoverGate(finalSet, targetSet); err != nil {
		e.actionLog(ctx, 0, "switchover", err.Error(), "blocked", "errant_gtid")
		rollback()
		e.Alert(ctx, "errant_gtid", fmt.Sprintf("switchover %s -> %s blocked: %s; %s writable again, epoch unchanged", oldPrimary, newPrimary, err, oldPrimary), true)
		return err
	}
	e.actionLog(ctx, 0, "switchover", "target caught up to fenced source; no errant GTIDs", "ok", "gtid_gate")
	if err := newAdm.StopReplica(ctx); err != nil {
		e.actionLog(ctx, 0, "switchover", err.Error(), "error", "stop_replica")
		rollback()
		return err
	}
	if err := newAdm.ResetReplicaAll(ctx); err != nil {
		e.actionLog(ctx, 0, "switchover", err.Error(), "error", "reset_replica")
		rollback()
		return err
	}
	if err := newAdm.PromoteWritable(ctx); err != nil {
		e.actionLog(ctx, 0, "switchover", err.Error(), "error", "promote_writable")
		rollback()
		return err
	}
	epoch, _ := e.store.GetEpoch(ctx)
	newEpoch, err := e.store.CasBumpEpoch(ctx, epoch)
	if err != nil {
		e.actionLog(ctx, epoch, "switchover", err.Error(), "error", "epoch_bump")
		// v0.3.0: never leave the new node writable behind a key that still
		// names the old one. Demote it and roll back if it took no writes.
		if rerr := e.undoPromote(ctx, oldAdm, newAdm, oldPrimary, newPrimary); rerr != nil {
			return fmt.Errorf("epoch bump failed (%v); %v", err, rerr)
		}
		rollback()
		return err
	}
	if err := e.setMasterRetry(ctx, newPrimary, newEpoch); err != nil {
		if cur, _, gerr := e.store.GetPrimary(ctx); gerr == nil && cur == newPrimary {
			// key written; only the writer service sync failed: keep going, the
			// leader's per-tick sync retries it.
			e.actionLog(ctx, newEpoch, "switchover", err.Error(), "warn", "writer_service")
		} else {
			e.actionLog(ctx, newEpoch, "switchover", err.Error(), "error", "set_master")
			if rerr := e.undoPromote(ctx, oldAdm, newAdm, oldPrimary, newPrimary); rerr != nil {
				return e.abortSwitchoverPostEpoch(ctx, newEpoch, oldPrimary, newPrimary, err.Error()+"; "+rerr.Error())
			}
			// key still names old (now read-only with every GTID): restore it
			// writable at a fresh epoch, exactly like the guard's restore_writer.
			_ = oldAdm.SetSuperReadOnly(ctx, false)
			_ = e.proxy.ApplyPromote(ctx, oldPrimary, newPrimary)
			// v0.3.3: the key/writer-current still carry the old epoch; rewrite
			// them for old at the new epoch, or leave a repair note for the leader
			// (otherwise it reads as a foreign master-key write).
			if serr := e.setMasterRetry(ctx, oldPrimary, newEpoch); serr != nil {
				if perr := e.store.PutKeyRepair(ctx, oldPrimary, newEpoch); perr != nil {
					e.log.Warn("key repair note", "err", perr)
				}
			}
			e.actionLog(ctx, newEpoch, "switchover", "set_master failed after epoch bump; new demoted, old restored writable (key unchanged)", "abort", "post_epoch_rollback")
			e.Alert(ctx, "switchover-aborted", fmt.Sprintf("switchover %s -> %s aborted after epoch bump (%s); %s kept as writer", oldPrimary, newPrimary, err, oldPrimary), true)
			return fmt.Errorf("switchover aborted: %s", err)
		}
	}
	if err := e.proxy.ApplyPromote(ctx, newPrimary, oldPrimary); err != nil {
		e.actionLog(ctx, newEpoch, "switchover", err.Error(), "warn", "proxysql_online_new")
	}
	srcHost, srcPort, _ := mysql.PeerHostPort(e.cfg, newPrimary)
	replUser, replPass := e.cfg.ReplCredentials()
	// needs-rebuild only if, after reconnecting, the actual source config is wrong.
	recovered, err := oldAdm.ChangeSourceVerified(ctx, srcHost, srcPort, replUser, replPass)
	if err != nil {
		// v0.3.0: the old primary was read-only with GTIDs ⊆ new's; a failed
		// re-point is a connectivity problem, not lost data. Only mark
		// needs-rebuild when the old node really has transactions the new lacks.
		oldSet, oerr := oldAdm.GTIDExecuted(ctx)
		newSet, nerr := newAdm.GTIDExecuted(ctx)
		if oerr == nil && nerr == nil && gtid.Subtract(oldSet, newSet).Empty() {
			return e.degraded(ctx, newEpoch, "switchover", fmt.Sprintf("%s -> %s promoted at epoch %d; re-pointing %s failed (%s), its data is a subset of the new primary: rejoin retries, no rebuild needed", oldPrimary, newPrimary, newEpoch, oldPrimary, err))
		}
		return e.abortSwitchoverPostEpoch(ctx, newEpoch, oldPrimary, newPrimary, err.Error())
	}
	if recovered {
		e.actionLog(ctx, newEpoch, "switchover", "change source statement errored but actual source verified after reconnect", "warn", "change_source_verified")
	}
	// START REPLICA failures are not data problems: retry, then let the
	// connected-wait below decide ok vs degraded.
	semiAfter := e.semiRejoinBefore(ctx, newAdm, oldAdm, "switchover", newEpoch)
	if n, verified, err := oldAdm.StartReplicaRetry(ctx, 5, 500*time.Millisecond); err != nil {
		e.actionLog(ctx, newEpoch, "switchover", err.Error(), "warn", "start_replica")
	} else if n > 1 || verified {
		e.actionLog(ctx, newEpoch, "switchover", fmt.Sprintf("START REPLICA succeeded on attempt %d (verified from thread state after error: %v)", n, verified), "warn", "start_replica_retry")
	}
	// Never report ok until the re-pointed replica is actually connected.
	if err := oldAdm.WaitReplicaConnected(ctx, e.cfg.ReplConnectTimeout); err != nil {
		return e.degraded(ctx, newEpoch, "switchover", fmt.Sprintf("%s -> %s promoted at epoch %d but replica %s not connected (user %s): %s", oldPrimary, newPrimary, newEpoch, oldPrimary, replUser, err))
	}
	if semiAfter {
		e.semiRejoinAfter(ctx, newAdm, newPrimary, "switchover", newEpoch)
	}
	e.actionLog(ctx, newEpoch, "switchover", fmt.Sprintf("%s -> %s complete at epoch %d, replica %s connected (user %s)", oldPrimary, newPrimary, newEpoch, oldPrimary, replUser), "ok", "end")
	e.Alert(ctx, "switchover", fmt.Sprintf("%s -> %s complete at epoch %d", oldPrimary, newPrimary, newEpoch), false)
	return nil
}

func (e *Executor) setMasterRetry(ctx context.Context, node string, epoch uint64) error {
	var err error
	for i := 0; i < 3; i++ {
		if err = e.store.SetMaster(ctx, node, epoch); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Duration(i+1) * 500 * time.Millisecond):
		}
	}
	return err
}

// undoPromote makes the new node read-only again; it refuses (keeps new
// writable) if new already holds transactions the old primary lacks.
func (e *Executor) undoPromote(ctx context.Context, oldAdm, newAdm *mysql.Admin, oldPrimary, newPrimary string) error {
	if err := newAdm.SetSuperReadOnly(ctx, true); err != nil {
		return fmt.Errorf("could not demote %s: %w", newPrimary, err)
	}
	oldSet, oerr := oldAdm.GTIDExecuted(ctx)
	newSet, nerr := newAdm.GTIDExecuted(ctx)
	if oerr != nil || nerr != nil {
		return fmt.Errorf("could not compare GTIDs (%v/%v)", oerr, nerr)
	}
	if extra := gtid.Subtract(newSet, oldSet); !extra.Empty() {
		_ = newAdm.SetSuperReadOnly(ctx, false)
		return fmt.Errorf("%s took writes during the switchover (%s); kept writable", newPrimary, extra)
	}
	// re-point the demoted node at the old primary again (best effort; the
	// guard's rejoin handles it otherwise).
	if h, p, err := mysql.PeerHostPort(e.cfg, oldPrimary); err == nil {
		u, pw := e.cfg.ReplCredentials()
		if _, err := newAdm.ChangeSourceVerified(ctx, h, p, u, pw); err == nil {
			_, _, _ = newAdm.StartReplicaRetry(ctx, 5, 500*time.Millisecond)
		} else {
			e.log.Warn("re-point demoted node", "node", newPrimary, "err", err)
		}
	}
	return nil
}

// degraded logs action/end as result=degraded, alerts, and returns an error so
// the CLI exits non-zero. The primary change itself is kept.
func (e *Executor) degraded(ctx context.Context, epoch uint64, action, msg string) error {
	e.actionLog(ctx, epoch, action, msg, "degraded", "end")
	e.log.Error(action+" degraded", "msg", msg)
	if e.alerts != nil {
		_ = e.alerts.Alert(ctx, action+"-degraded", msg, true)
	}
	return fmt.Errorf("%s degraded: %s", action, msg)
}

// abortSwitchoverPostEpoch keeps the new primary, marks old needs-rebuild, alerts.
func (e *Executor) abortSwitchoverPostEpoch(ctx context.Context, epoch uint64, oldPrimary, newPrimary, reason string) error {
	msg := fmt.Sprintf("post-epoch abort: old %s needs-rebuild (%s); new primary %s kept at epoch %d", oldPrimary, reason, newPrimary, epoch)
	e.actionLog(ctx, epoch, "switchover_abort", msg, "abort", "post_epoch")
	_ = e.store.MarkNeedsRebuild(ctx, oldPrimary, reason)
	if e.alerts != nil {
		_ = e.alerts.Alert(ctx, "needs-rebuild", msg, true)
	}
	return fmt.Errorf("%s", msg)
}

func (e *Executor) actionLog(ctx context.Context, epoch uint64, action, reason, result, step string) {
	if e.alog != nil {
		e.alog.Log(ctx, epoch, action, reason, result, step)
	}
}

// SwitchoverGate is the post-fence check: after catch-up the target must hold
// every source GTID and nothing the fenced source lacks.
func SwitchoverGate(final, target gtid.Set) error {
	if missing := gtid.Subtract(final, target); !missing.Empty() {
		return fmt.Errorf("target missing source GTIDs %s", missing)
	}
	if extra := gtid.Subtract(target, final); !extra.Empty() {
		return fmt.Errorf("target has errant GTIDs %s", extra)
	}
	return nil
}
