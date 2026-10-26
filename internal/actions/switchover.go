package actions

import (
	"context"
	"fmt"

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
	// The new primary must not hold transactions the old primary lacks; any lag
	// is drained after the old primary is made read-only.
	if extra := gtid.Subtract(peer, local); !extra.Empty() {
		e.actionLog(ctx, 0, "switchover", "new primary has GTIDs missing on old primary: "+string(extra), "blocked", "precheck")
		return fmt.Errorf("switchover blocked: target has extra GTIDs %s", extra)
	}
	if err := e.checkErrant(ctx, peer, local, "switchover"); err != nil {
		e.actionLog(ctx, 0, "switchover", err.Error(), "blocked", "errant_gtid")
		return err
	}
	e.actionLog(ctx, 0, "switchover", "starting", "ok", "begin")
	if e.observe {
		e.actionLog(ctx, 0, "switchover", "observe mode", "skipped", "end")
		return nil
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
	rolled := false
	rollback := func() {
		if rolled {
			return
		}
		rolled = true
		_ = oldAdm.SetSuperReadOnly(ctx, false)
		_ = e.proxy.ApplyPromote(ctx, oldPrimary, newPrimary)
		e.actionLog(ctx, 0, "switchover", "pre-epoch rollback", "ok", "rollback")
	}
	if err := oldAdm.SetSuperReadOnly(ctx, true); err != nil {
		e.actionLog(ctx, 0, "switchover", err.Error(), "error", "old_super_read_only")
		return err
	}
	e.actionLog(ctx, 0, "switchover", "old primary read-only", "ok", "old_super_read_only")
	if err := e.proxy.ApplyPromote(ctx, newPrimary, oldPrimary); err != nil {
		e.actionLog(ctx, 0, "switchover", err.Error(), "error", "proxysql_offline_old")
		rollback()
		return err
	}
	targetGTID, err := oldAdm.GTIDExecuted(ctx)
	if err != nil {
		e.actionLog(ctx, 0, "switchover", err.Error(), "error", "read_old_gtid")
		rollback()
		return err
	}
	if err := newAdm.WaitExecuted(ctx, targetGTID, e.cfg.MaxApplyLag*2); err != nil {
		e.actionLog(ctx, 0, "switchover", err.Error(), "error", "wait_gtid")
		rollback()
		return err
	}
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
		return err
	}
	if err := e.store.SetMaster(ctx, newPrimary, newEpoch); err != nil {
		return e.abortSwitchoverPostEpoch(ctx, newEpoch, oldPrimary, newPrimary, err.Error())
	}
	if err := e.proxy.ApplyPromote(ctx, newPrimary, oldPrimary); err != nil {
		e.actionLog(ctx, newEpoch, "switchover", err.Error(), "warn", "proxysql_online_new")
	}
	srcHost, srcPort, _ := mysql.PeerHostPort(e.cfg, newPrimary)
	if err := oldAdm.ChangeSourceAuto(ctx, srcHost, srcPort, e.cfg.MySQLAdminUser, e.cfg.MySQLAdminPass); err != nil {
		return e.abortSwitchoverPostEpoch(ctx, newEpoch, oldPrimary, newPrimary, err.Error())
	}
	if err := oldAdm.StartReplica(ctx); err != nil {
		return e.abortSwitchoverPostEpoch(ctx, newEpoch, oldPrimary, newPrimary, err.Error())
	}
	e.actionLog(ctx, newEpoch, "switchover", fmt.Sprintf("%s -> %s complete at epoch %d", oldPrimary, newPrimary, newEpoch), "ok", "end")
	return nil
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
