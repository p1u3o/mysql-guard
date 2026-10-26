package guard

import (
	"context"
	"time"

	"mysql-guard/internal/config"
)

// v030Checks runs on the leader each tick:
//   - writer service sync (never in observe mode or maintenance),
//   - foreign master-key write detection (alert only),
//   - read-only ProxySQL writer-hostgroup verification (alert only).
func (r *Runtime) v030Checks(ctx context.Context, now time.Time, maint bool, primary string, epoch uint64) {
	if primary != "" && r.cfg.WriterServiceName != "" && r.cfg.Mode != config.ModeObserve && !maint {
		wrote, err := r.store.SyncWriterService(ctx, primary, epoch, "guard-sync")
		switch {
		case err != nil:
			if err.Error() != r.svcLastErr {
				r.svcLastErr = err.Error()
				r.exec.Alert(ctx, "writer_service_sync_failed", r.cfg.WriterServiceName+": "+err.Error(), true)
			}
		case wrote:
			r.svcLastErr = ""
			r.log.Info("writer service synced", "service", r.cfg.WriterServiceName, "primary", primary, "epoch", epoch)
			r.exec.LogAction(ctx, epoch, "writer_service", r.cfg.WriterServiceName+" -> "+primary, "ok", "sync")
		default:
			r.svcLastErr = ""
		}
	}

	if r.cfg.ForeignWriteAlert {
		val, foreign, err := r.store.ForeignWriter(ctx)
		if err == nil && foreign {
			r.foreignTicks++
			// two consecutive ticks: skips the window between epoch bump and SetMaster
			if r.foreignTicks >= 2 && r.foreignAlert != val {
				r.foreignAlert = val
				r.exec.Alert(ctx, "foreign_master_write", "master key "+r.store.Keys().MasterKV()+" = "+val+" was not written by a guard (repman/bash post-script?); the guard must be the sole promoter", true)
			}
		} else if err == nil {
			if r.foreignAlert != "" {
				r.exec.Alert(ctx, "foreign_master_write_cleared", "master key consistent with guard epoch again", false)
			}
			r.foreignTicks, r.foreignAlert = 0, ""
		}
	}

	if r.proxy != nil && r.proxy.VerifyEnabled() && primary != "" {
		want := r.proxy.ExpectedEndpoint(primary)
		online, err := r.proxy.WriterHostgroup(ctx)
		msg := ""
		if err != nil {
			msg = "ProxySQL admin query failed: " + err.Error()
		} else {
			msg = proxysqlMismatch(online, want)
		}
		if msg == "" {
			if r.pxAlerted != "" {
				r.exec.Alert(ctx, "proxysql_writer_ok", "ProxySQL writer hostgroup matches "+primary+" again", false)
			}
			r.pxSince, r.pxAlerted = time.Time{}, ""
			return
		}
		if r.pxSince.IsZero() {
			r.pxSince = now
		}
		if now.Sub(r.pxSince) >= r.cfg.ProxySQLVerifyGrace && r.pxAlerted == "" {
			r.pxAlerted = msg
			r.exec.Alert(ctx, "proxysql_writer_mismatch", msg, true)
		}
	}
}

func proxysqlMismatch(online []string, want string) string {
	if want == "" {
		return ""
	}
	if len(online) == 1 && online[0] == want {
		return ""
	}
	return "ProxySQL writer hostgroup ONLINE=" + join(online) + ", writer key says " + want
}

func join(a []string) string {
	out := "["
	for i, s := range a {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out + "]"
}
