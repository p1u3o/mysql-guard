package actions

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"mysql-guard/internal/config"
	"mysql-guard/internal/consul"
	"mysql-guard/internal/decision"
	"mysql-guard/internal/errant"
	"mysql-guard/internal/gtid"
	"mysql-guard/internal/mysql"
	"mysql-guard/internal/notify"
	"mysql-guard/internal/proxysql"
	"mysql-guard/internal/replhealth"
)

// Executor performs fence/promote/register when mode allows.
type Executor struct {
	cfg       *config.Config
	log       *slog.Logger
	mysql     *mysql.Prober
	peers     *mysql.Pool
	proxy     *proxysql.Client
	store     *consul.Store
	alerts    *notify.Fanout
	alog      *Logger
	observe   bool
	fenceOnly bool

	replMu      sync.Mutex
	replInit    bool
	lastReplKey string
}

func NewExecutor(cfg *config.Config, log *slog.Logger, my *mysql.Prober, peers *mysql.Pool, px *proxysql.Client, store *consul.Store, alerts *notify.Fanout) *Executor {
	return &Executor{
		cfg:       cfg,
		log:       log,
		mysql:     my,
		peers:     peers,
		proxy:     px,
		store:     store,
		alerts:    alerts,
		alog:      NewLogger(store, cfg.NodeName, cfg.ActionLogMax),
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
	if on, msg := e.maintenance(ctx); on {
		e.actionLog(ctx, epoch, "promote", msg, "blocked", "maintenance")
		e.log.Info("promote skipped", "reason", "maintenance")
		return nil
	}
	if err := e.checkErrant(ctx, local, primaryExecuted, "promote"); err != nil {
		e.actionLog(ctx, epoch, "promote", err.Error(), "blocked", "errant_gtid")
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
	if !e.replAlertsChanged(alerts) {
		return
	}
	if len(alerts) == 0 {
		e.log.Info("replication", "code", "ok", "msg", st.Node+": replication alerts cleared")
	}
	for _, a := range alerts {
		e.log.Warn("replication", "code", a.Code, "msg", a.Message)
		if e.alerts != nil {
			_ = e.alerts.Alert(ctx, a.Code, a.Message, a.Severity == "critical")
		}
	}
}

// replAlertsChanged reports whether the set of alert codes differs from the last
// tick, so a persistent condition is logged and alerted once, not every tick.
func (e *Executor) replAlertsChanged(alerts []replhealth.Alert) bool {
	codes := make([]string, 0, len(alerts))
	for _, a := range alerts {
		codes = append(codes, a.Code)
	}
	key := strings.Join(codes, ",")
	e.replMu.Lock()
	defer e.replMu.Unlock()
	if e.replInit && key == e.lastReplKey {
		return false
	}
	e.replInit, e.lastReplKey = true, key
	return true
}
