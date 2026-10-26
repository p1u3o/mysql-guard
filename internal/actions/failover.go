package actions

import (
	"context"
	"fmt"

	"mysql-guard/internal/gtid"
)

// FenceNode makes a peer super_read_only via its remote connection.
func (e *Executor) FenceNode(ctx context.Context, node string, epoch uint64, reason string) error {
	if on, msg := e.maintenance(ctx); on {
		e.actionLog(ctx, epoch, "fence", msg, "blocked", "maintenance")
		return nil
	}
	if e.observe {
		e.actionLog(ctx, epoch, "fence", reason+" (observe mode)", "skipped", "super_read_only")
		return nil
	}
	if e.peers == nil {
		return fmt.Errorf("peer pool not configured")
	}
	adm, err := e.peers.Admin(node)
	if err != nil {
		return err
	}
	if err := adm.SetSuperReadOnly(ctx, true); err != nil {
		e.actionLog(ctx, epoch, "fence", err.Error(), "error", "super_read_only")
		return err
	}
	if err := e.proxy.ApplyFence(ctx, node); err != nil {
		e.log.Warn("proxysql fence", "err", err)
	}
	if e.alerts != nil {
		_ = e.alerts.Alert(ctx, "fence", reason, true)
	}
	_ = e.store.MarkFenced(ctx, node, epoch)
	e.actionLog(ctx, epoch, "fence", reason, "ok", "super_read_only")
	return nil
}

// Failover promotes candidate after the primary was voted dead.
// The candidate must first apply every GTID it already received.
func (e *Executor) Failover(ctx context.Context, oldPrimary, candidate string, epoch uint64) error {
	if on, msg := e.maintenance(ctx); on {
		e.actionLog(ctx, epoch, "failover", msg, "blocked", "maintenance")
		return nil
	}
	reason := fmt.Sprintf("primary %s voted dead; candidate %s", oldPrimary, candidate)
	if e.observe || e.fenceOnly {
		e.actionLog(ctx, epoch, "failover", reason+" (mode "+string(e.cfg.Mode)+")", "skipped", "end")
		return nil
	}
	if e.peers == nil {
		return fmt.Errorf("peer pool not configured")
	}
	adm, err := e.peers.Admin(candidate)
	if err != nil {
		return err
	}
	e.actionLog(ctx, epoch, "failover", reason, "ok", "begin")
	st, err := e.peers.ReadState(ctx, candidate)
	if err != nil {
		e.actionLog(ctx, epoch, "failover", err.Error(), "error", "read_candidate")
		return err
	}
	if err := adm.WaitExecuted(ctx, gtid.Parse(st.Retrieved), e.cfg.MaxApplyLag); err != nil {
		e.actionLog(ctx, epoch, "failover", err.Error(), "blocked", "gtid_gate")
		return err
	}
	e.actionLog(ctx, epoch, "failover", "candidate applied all received GTIDs", "ok", "gtid_gate")
	if err := adm.StopReplica(ctx); err != nil {
		e.actionLog(ctx, epoch, "failover", err.Error(), "error", "stop_replica")
		return err
	}
	if err := adm.ResetReplicaAll(ctx); err != nil {
		e.actionLog(ctx, epoch, "failover", err.Error(), "error", "reset_replica")
		return err
	}
	if err := adm.PromoteWritable(ctx); err != nil {
		e.actionLog(ctx, epoch, "failover", err.Error(), "error", "promote_writable")
		return err
	}
	newEpoch, err := e.store.CasBumpEpoch(ctx, epoch)
	if err != nil {
		e.actionLog(ctx, epoch, "failover", err.Error(), "error", "epoch_bump")
		return err
	}
	if err := e.store.SetMaster(ctx, candidate, newEpoch); err != nil {
		e.actionLog(ctx, newEpoch, "failover", err.Error(), "error", "set_master")
		return err
	}
	if err := e.proxy.ApplyPromote(ctx, candidate, oldPrimary); err != nil {
		e.actionLog(ctx, newEpoch, "failover", err.Error(), "warn", "proxysql")
	}
	if e.alerts != nil {
		_ = e.alerts.Alert(ctx, "failover", reason, true)
	}
	e.actionLog(ctx, newEpoch, "failover", fmt.Sprintf("%s promoted at epoch %d", candidate, newEpoch), "ok", "end")
	return nil
}
