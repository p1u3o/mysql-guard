package actions

import (
	"context"
	"fmt"
	"time"

	"mysql-guard/internal/consul"
	"mysql-guard/internal/mysql"
)

// Semi-sync handling (v0.3.6, -semisync-restore):
//   - failover: the candidate is promoted with semi-sync source OFF (no
//     replica is left to acknowledge; ON would stall every commit for
//     rpl_semi_sync_source_timeout). The Consul record gets Pending=true.
//   - rejoin: the joiner gets replica ON before START REPLICA; once it is
//     connected the primary gets source ON (when Pending or already enabled).
//   - switchover: when the old primary had source enabled, the old primary
//     gets replica ON before START REPLICA and the new one source ON after the
//     replica connected.
// Failures are action-logged as warn; they never fail the action.

func (e *Executor) semiRecord(ctx context.Context) *consul.SemiSyncRecord {
	if e.store == nil {
		return nil
	}
	r, err := e.store.GetSemiSync(ctx)
	if err != nil {
		e.log.Warn("semisync kv read", "err", err)
	}
	return r
}

// semiFailoverPromote runs on the candidate before it becomes writable.
func (e *Executor) semiFailoverPromote(ctx context.Context, adm *mysql.Admin, st mysql.GTIDState, candidate string, epoch uint64) {
	if !e.cfg.SemiSyncRestore || !st.SemiSync.Known || !st.SemiSync.Installed {
		return
	}
	rec := e.semiRecord(ctx)
	enabled := st.SemiSync.ReplicaEnabled || (rec != nil && (rec.Enabled || rec.Pending))
	if !enabled {
		return
	}
	if err := adm.SetSemiSync(ctx, false, false); err != nil {
		e.actionLog(ctx, epoch, "failover", "semi-sync off on new primary: "+err.Error(), "warn", "semisync")
	}
	if e.store != nil {
		_ = e.store.PutSemiSync(ctx, consul.SemiSyncRecord{Enabled: true, Pending: true, Mode: "off", Node: candidate, Epoch: epoch, At: time.Now().UTC()})
	}
	e.actionLog(ctx, epoch, "failover", "semi-sync was enabled: new primary runs async until a replica rejoins, then semi-sync is re-enabled", "ok", "semisync")
}

// semiRejoinBefore runs before START REPLICA on the joiner; returns whether
// the source must get semi-sync source ON after the joiner connected.
func (e *Executor) semiRejoinBefore(ctx context.Context, sourceAdm, joinerAdm *mysql.Admin, action string, epoch uint64) bool {
	if !e.cfg.SemiSyncRestore {
		return false
	}
	src := sourceAdm.ReadSemiSync(ctx)
	old := joinerAdm.ReadSemiSync(ctx)
	rec := e.semiRecord(ctx)
	want := SemiWanted(src, old, rec)
	if !want {
		return false
	}
	if err := joinerAdm.SetSemiSync(ctx, false, true); err != nil {
		e.actionLog(ctx, epoch, action, "semi-sync replica on: "+err.Error(), "warn", "semisync")
		return false
	}
	return true
}

func (e *Executor) semiRejoinAfter(ctx context.Context, sourceAdm *mysql.Admin, source, action string, epoch uint64) {
	if err := sourceAdm.SetSemiSync(ctx, true, false); err != nil {
		e.actionLog(ctx, epoch, action, "semi-sync source on: "+err.Error(), "warn", "semisync")
		return
	}
	if e.store != nil {
		_ = e.store.PutSemiSync(ctx, consul.SemiSyncRecord{Enabled: true, Mode: "sync", Node: source, Epoch: epoch, At: time.Now().UTC()})
	}
	e.actionLog(ctx, epoch, action, fmt.Sprintf("semi-sync re-enabled (source %s)", source), "ok", "semisync")
}

// SemiSyncNote formats the failover-alert note.
func SemiSyncNote(mode string, age time.Duration) string {
	if mode == "" {
		mode = "unknown"
	}
	if age > 0 {
		return fmt.Sprintf("semi-sync at loss: %s (last sample %s before)", mode, age.Round(time.Second))
	}
	return "semi-sync at loss: " + mode
}

// SemiWanted: semi-sync follows the cluster, not the node. It is wanted when
// either side has source enabled (a switchover's old primary is the joiner)
// or the Consul record says the cluster runs (or should run) semi-sync.
func SemiWanted(src, joiner mysql.SemiSync, rec *consul.SemiSyncRecord) bool {
	return (src.Known && src.SourceEnabled) || (joiner.Known && joiner.SourceEnabled) ||
		(rec != nil && (rec.Enabled || rec.Pending))
}
