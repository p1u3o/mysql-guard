package actions

import (
	"context"
	"errors"
	"fmt"
	"time"

	"mysql-guard/internal/config"
	"mysql-guard/internal/consul"
)

// Semi-sync stall handling (-semisync-stall). The decision is made by
// internal/semistall; these methods apply it with the same guards as every
// other action: not in observe/fence mode, not in maintenance, not while a
// failover, fence or switchover is in flight, and only for the primary and
// epoch the decision was made for. Every outcome is action-logged.

// ErrSemiStallSkipped marks a decision that was safe to drop (state changed
// between decision and action).
var ErrSemiStallSkipped = errors.New("semi-sync stall action skipped")

// TransitionHeld reports whether a failover/switchover transition is in flight
// (or ended within the grace window).
func (e *Executor) TransitionHeld(ctx context.Context) bool { return e.transitionHeld(ctx) }

func (e *Executor) semiStallGuard(ctx context.Context, primary string, epoch uint64) error {
	if e.cfg.Mode != config.ModeAct {
		return fmt.Errorf("%w: mode %s", ErrSemiStallSkipped, e.cfg.Mode)
	}
	if e.peers == nil || e.store == nil {
		return fmt.Errorf("%w: no peer pool or store", ErrSemiStallSkipped)
	}
	if on, _ := e.maintenance(ctx); on {
		return fmt.Errorf("%w: maintenance mode", ErrSemiStallSkipped)
	}
	if e.transitionHeld(ctx) {
		return fmt.Errorf("%w: failover or switchover in flight", ErrSemiStallSkipped)
	}
	cur, curEpoch, err := e.store.GetPrimary(ctx)
	if err != nil {
		return fmt.Errorf("%w: read writer key: %v", ErrSemiStallSkipped, err)
	}
	if cur != primary || curEpoch != epoch {
		return fmt.Errorf("%w: writer changed to %s epoch %d", ErrSemiStallSkipped, cur, curEpoch)
	}
	return nil
}

// SemiStallDisable turns semi-sync source off on the primary because the
// replica is confirmed dead, and records that the guard did it.
func (e *Executor) SemiStallDisable(ctx context.Context, primary string, epoch uint64, reason string) error {
	if err := e.semiStallGuard(ctx, primary, epoch); err != nil {
		e.actionLog(ctx, epoch, "semisync_stall", err.Error(), "skipped", "off")
		return err
	}
	adm, err := e.peers.Admin(primary)
	if err != nil {
		return err
	}
	if err := adm.SetSemiSyncSource(ctx, false); err != nil {
		e.actionLog(ctx, epoch, "semisync_stall", "set rpl_semi_sync_source_enabled=OFF on "+primary+": "+err.Error(), "error", "off")
		return err
	}
	if ss := adm.ReadSemiSync(ctx); ss.Known && ss.SourceEnabled {
		err := fmt.Errorf("rpl_semi_sync_source_enabled still ON on %s after SET", primary)
		e.actionLog(ctx, epoch, "semisync_stall", err.Error(), "error", "off")
		return err
	}
	if perr := e.store.PutSemiSync(ctx, consul.SemiSyncRecord{Enabled: true, Pending: true, StallDisabled: true, Mode: "off", Node: primary, Epoch: epoch, At: time.Now().UTC()}); perr != nil {
		// The switch is off but the record is not: the guard would never turn it
		// on again. Undo rather than leave an unowned OFF.
		_ = adm.SetSemiSyncSource(ctx, true)
		err := fmt.Errorf("could not record the disable in Consul (%v); semi-sync source turned back on", perr)
		e.actionLog(ctx, epoch, "semisync_stall", err.Error(), "error", "off")
		return err
	}
	msg := fmt.Sprintf("semi-sync source turned off on %s: %s; writes no longer wait for a replica acknowledgement until it is back and caught up", primary, reason)
	e.actionLog(ctx, epoch, "semisync_stall", msg, "ok", "off")
	e.Alert(ctx, "semisync_stall_off", msg, false)
	return nil
}

// SemiStallReenable turns semi-sync source on again. It verifies that a
// semi-sync client registered; if none did within the wait, it turns the
// switch off again (commits would otherwise stall for the timeout) and
// returns an error so the caller backs off.
func (e *Executor) SemiStallReenable(ctx context.Context, primary string, epoch uint64, reason string) error {
	if err := e.semiStallGuard(ctx, primary, epoch); err != nil {
		e.actionLog(ctx, epoch, "semisync_stall", err.Error(), "skipped", "on")
		return err
	}
	adm, err := e.peers.Admin(primary)
	if err != nil {
		return err
	}
	if err := adm.SetSemiSyncSource(ctx, true); err != nil {
		e.actionLog(ctx, epoch, "semisync_stall", "set rpl_semi_sync_source_enabled=ON on "+primary+": "+err.Error(), "error", "on")
		return err
	}
	registered := false
	deadline := time.Now().Add(3 * time.Second)
	for {
		if ss := adm.ReadSemiSync(ctx); ss.Known && ss.SourceEnabled && ss.SourceClients > 0 {
			registered = true
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !registered {
		_ = adm.SetSemiSyncSource(context.WithoutCancel(ctx), false)
		err := fmt.Errorf("no semi-sync replica registered within 3s of turning the source on; turned it off again (replica IO thread probably started without semi-sync)")
		e.actionLog(ctx, epoch, "semisync_stall", err.Error(), "warn", "on")
		return err
	}
	if perr := e.store.PutSemiSync(ctx, consul.SemiSyncRecord{Enabled: true, Mode: "sync", Node: primary, Epoch: epoch, At: time.Now().UTC()}); perr != nil {
		e.log.Warn("semisync kv write", "err", perr)
	}
	msg := fmt.Sprintf("semi-sync source turned on again on %s: %s", primary, reason)
	e.actionLog(ctx, epoch, "semisync_stall", msg, "ok", "on")
	e.Alert(ctx, "semisync_stall_on", msg, false)
	return nil
}

// SemiStallClear forgets a guard-made disable that someone else already undid.
func (e *Executor) SemiStallClear(ctx context.Context, primary string, epoch uint64) {
	if e.store == nil || e.cfg.Mode != config.ModeAct {
		return
	}
	if perr := e.store.PutSemiSync(ctx, consul.SemiSyncRecord{Enabled: true, Mode: "sync", Node: primary, Epoch: epoch, At: time.Now().UTC()}); perr != nil {
		e.log.Warn("semisync kv write", "err", perr)
		return
	}
	e.actionLog(ctx, epoch, "semisync_stall", "semi-sync source was turned on outside the guard; stall-disable flag cleared", "ok", "clear")
}
