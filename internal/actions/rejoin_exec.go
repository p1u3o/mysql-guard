package actions

import (
	"context"
	"fmt"

	"mysql-guard/internal/gtid"
	"mysql-guard/internal/mysql"
	"mysql-guard/internal/rejoin"
)

// Rejoin uses source connection for gtid_purged and joiner for replication setup.
func (e *Executor) Rejoin(ctx context.Context, sourcePeer string, joinerPeer string, local, clusterPrimary gtid.Set, readOnly bool) (rejoin.Plan, error) {
	if err := e.checkErrant(ctx, local, clusterPrimary, "rejoin"); err != nil {
		e.actionLog(ctx, 0, "rejoin", err.Error(), "blocked", "errant_gtid")
		return rejoin.Plan{Outcome: rejoin.OutcomeBlockedErrant, Reason: err.Error()}, err
	}
	plan := rejoin.Evaluate(local, clusterPrimary, readOnly)
	if plan.Outcome != rejoin.OutcomeAutoPosition {
		if plan.Alert != "" {
			e.actionLog(ctx, 0, "rejoin", plan.Alert, "blocked", string(plan.Outcome))
			if e.alerts != nil {
				_ = e.alerts.Alert(ctx, string(plan.Outcome), plan.Alert, true)
			}
		}
		return plan, fmt.Errorf("%s", plan.Reason)
	}
	e.actionLog(ctx, 0, "rejoin", "starting auto-position", "ok", "begin")
	if e.observe {
		e.actionLog(ctx, 0, "rejoin", "observe mode", "skipped", "end")
		return plan, nil
	}
	if e.peers == nil {
		return plan, fmt.Errorf("peer pool not configured")
	}
	if joinerPeer == "" {
		joinerPeer = e.cfg.NodeName
	}
	sourceAdm, err := e.peers.Admin(sourcePeer)
	if err != nil {
		return plan, err
	}
	joinerAdm, err := e.peers.Admin(joinerPeer)
	if err != nil {
		return plan, err
	}
	sourcePurged, err := sourceAdm.GTIDPurged(ctx)
	if err != nil {
		e.actionLog(ctx, 0, "rejoin", err.Error(), "error", "source_gtid_purged")
		return plan, err
	}
	joinerExecuted, err := joinerAdm.GTIDExecuted(ctx)
	if err != nil {
		e.actionLog(ctx, 0, "rejoin", err.Error(), "error", "joiner_gtid_executed")
		return plan, err
	}
	sourceExecuted, err := sourceAdm.GTIDExecuted(ctx)
	if err != nil {
		return plan, err
	}
	if mysql.PurgedBlocksRejoin(sourcePurged, joinerExecuted, sourceExecuted) {
		msg := "source missing binlogs for joiner gtid_executed (gtid_purged gap)"
		e.actionLog(ctx, 0, "rejoin", msg, "blocked", "needs_rebuild")
		_ = e.store.MarkNeedsRebuild(ctx, joinerPeer, msg)
		if e.alerts != nil {
			_ = e.alerts.Alert(ctx, "needs-rebuild", msg, true)
		}
		return rejoin.Plan{Outcome: rejoin.OutcomeNeedsRebuild, Reason: msg, Alert: msg}, fmt.Errorf("%s", msg)
	}
	host, port, _ := mysql.PeerHostPort(e.cfg, sourcePeer)
	if err := joinerAdm.ChangeSourceAuto(ctx, host, port, e.cfg.MySQLAdminUser, e.cfg.MySQLAdminPass); err != nil {
		e.actionLog(ctx, 0, "rejoin", err.Error(), "error", "change_source")
		return plan, err
	}
	if err := joinerAdm.StartReplica(ctx); err != nil {
		e.actionLog(ctx, 0, "rejoin", err.Error(), "error", "start_replica")
		return plan, err
	}
	e.actionLog(ctx, 0, "rejoin", "complete", "ok", "end")
	return plan, nil
}
