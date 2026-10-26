package actions

import (
	"context"
	"fmt"
	"log/slog"

	"mysql-guard/internal/config"
	"mysql-guard/internal/consul"
	"mysql-guard/internal/decision"
	"mysql-guard/internal/errant"
	"mysql-guard/internal/gtid"
	"mysql-guard/internal/mysql"
	"mysql-guard/internal/notify"
	"mysql-guard/internal/proxysql"
	"mysql-guard/internal/rejoin"
	"mysql-guard/internal/replhealth"
)

// Executor performs fence/promote/register when mode allows.
type Executor struct {
	cfg      *config.Config
	log      *slog.Logger
	mysql    *mysql.Prober
	proxy    *proxysql.Client
	store    *consul.Store
	alerts   *notify.Fanout
	observe  bool
	fenceOnly bool
}

func NewExecutor(cfg *config.Config, log *slog.Logger, my *mysql.Prober, px *proxysql.Client, store *consul.Store, alerts *notify.Fanout) *Executor {
	return &Executor{
		cfg:       cfg,
		log:       log,
		mysql:     my,
		proxy:     px,
		store:     store,
		alerts:    alerts,
		observe:   cfg.Mode == config.ModeObserve,
		fenceOnly: cfg.Mode == config.ModeFence,
	}
}

func (e *Executor) maintenance(ctx context.Context) (bool, string) {
	if e.store == nil {
		return false, ""
	}
	on, msg, err := e.store.MaintenanceEnabled(ctx)
	if err != nil {
		e.log.Warn("maintenance kv read", "err", err)
	}
	return on, msg
}

func (e *Executor) checkErrant(ctx context.Context, local, primary gtid.Set, action string) error {
	r := errant.BlocksPromotion(local, primary)
	if r.Errant {
		msg := fmt.Sprintf("%s blocked: %s", action, r.Reason)
		e.log.Error(msg)
		if e.alerts != nil {
			_ = e.alerts.Alert(ctx, "errant_gtid", msg, true)
		}
		return fmt.Errorf("%s", msg)
	}
	return nil
}

func (e *Executor) Fence(ctx context.Context, node string, epoch uint64, oldPrimary string, reason string) error {
	if on, _ := e.maintenance(ctx); on {
		e.log.Info("fence skipped", "reason", "maintenance")
		return nil
	}
	e.log.Info("fence", "node", node, "epoch", epoch, "reason", reason, "would_only", e.observe)
	if e.observe {
		return nil
	}
	if err := e.mysql.SetSuperReadOnly(ctx); err != nil {
		return err
	}
	if err := e.proxy.ApplyFence(ctx, node); err != nil {
		e.log.Warn("proxysql fence", "err", err)
	}
	if e.alerts != nil {
		_ = e.alerts.Alert(ctx, "fence", reason, true)
	}
	return e.store.MarkFenced(ctx, node, epoch)
}

func (e *Executor) Promote(ctx context.Context, candidate, oldPrimary string, local, primaryExecuted gtid.Set, epoch uint64) error {
	if on, _ := e.maintenance(ctx); on {
		e.log.Info("promote skipped", "reason", "maintenance")
		return nil
	}
	if err := e.checkErrant(ctx, local, primaryExecuted, "promote"); err != nil {
		return err
	}
	e.log.Info("promote", "candidate", candidate, "epoch", epoch, "would_only", e.observe || e.fenceOnly)
	if e.observe || e.fenceOnly {
		return nil
	}
	if !e.cfg.Promotable {
		return nil
	}
	if err := e.mysql.PromoteWritable(ctx); err != nil {
		return err
	}
	if err := e.proxy.ApplyPromote(ctx, candidate, oldPrimary); err != nil {
		e.log.Warn("proxysql promote", "err", err)
	}
	newEpoch, err := e.store.CasBumpEpoch(ctx, epoch)
	if err != nil {
		return err
	}
	return e.store.SetMaster(ctx, candidate, newEpoch)
}

func (e *Executor) RunScenario(ctx context.Context, res decision.ScenarioResult, candidate, oldPrimary string, local, primary gtid.Set, epoch uint64) {
	switch res.Action {
	case "promote":
		if err := e.Promote(ctx, candidate, oldPrimary, local, primary, epoch); err != nil {
			e.log.Error("promote failed", "err", err)
		}
	case "fence":
		if err := e.Fence(ctx, e.cfg.NodeName, epoch, oldPrimary, res.Reason); err != nil {
			e.log.Error("fence failed", "err", err)
		}
	case "warn", "alert":
		e.log.Warn("decision", "action", res.Action, "reason", res.Reason)
		if e.alerts != nil {
			_ = e.alerts.Alert(ctx, res.Action, res.Reason, res.Action == "alert")
		}
	default:
	}
}

func (e *Executor) PublishReplicationAlerts(ctx context.Context, st replhealth.State) {
	alerts := replhealth.Evaluate(st, replhealth.Config{MaxLagSeconds: e.cfg.ReplLagAlertSec})
	for _, a := range alerts {
		e.log.Warn("replication", "code", a.Code, "msg", a.Message)
		if e.alerts != nil {
			_ = e.alerts.Alert(ctx, a.Code, a.Message, a.Severity == "critical")
		}
	}
}

// Switchover performs planned handover with GTID-equal and errant checks.
func (e *Executor) Switchover(ctx context.Context, newPrimary string, local, peer gtid.Set) error {
	if on, _ := e.maintenance(ctx); on {
		return fmt.Errorf("maintenance mode")
	}
	if !gtid.Subtract(local, peer).Empty() || !gtid.Subtract(peer, local).Empty() {
		return fmt.Errorf("switchover requires equal GTID sets")
	}
	if err := e.checkErrant(ctx, local, peer, "switchover"); err != nil {
		return err
	}
	e.log.Info("switchover", "new_primary", newPrimary, "mode", e.cfg.Mode)
	if e.observe {
		return nil
	}
	return e.Promote(ctx, newPrimary, e.cfg.NodeName, local, peer, 0)
}

// Rejoin checks errant GTIDs and returns a rejoin plan (auto-position or needs-rebuild).
func (e *Executor) Rejoin(ctx context.Context, source string, local, clusterPrimary gtid.Set, readOnly bool) (rejoin.Plan, error) {
	if err := e.checkErrant(ctx, local, clusterPrimary, "rejoin"); err != nil {
		return rejoin.Plan{Outcome: rejoin.OutcomeBlockedErrant, Reason: err.Error()}, err
	}
	plan := rejoin.Evaluate(local, clusterPrimary, readOnly)
	if plan.Alert != "" && e.alerts != nil {
		_ = e.alerts.Alert(ctx, string(plan.Outcome), plan.Alert, plan.Outcome != rejoin.OutcomeAutoPosition)
	}
	e.log.Info("rejoin plan", "outcome", plan.Outcome, "reason", plan.Reason, "source", source, "mode", e.cfg.Mode)
	if e.observe || plan.Outcome != rejoin.OutcomeAutoPosition {
		return plan, nil
	}
	// TODO: CHANGE REPLICATION SOURCE TO ... SOURCE_AUTO_POSITION=1
	return plan, nil
}
