package actions

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"mysql-guard/internal/gtid"
	"mysql-guard/internal/mysql"
	"mysql-guard/internal/proxysql"
)

// ---- planned switchover: preflight and plan ----

// Check is one preflight result. Warn checks are reported but never block.
type Check struct {
	Name   string
	OK     bool
	Warn   bool
	Detail string
}

// PreflightInput is everything the preflight needs, already read. Pure data so
// EvaluatePreflight can be tested without MySQL or Consul.
type PreflightInput struct {
	Old, New       string
	CurrentPrimary string // from the writer key ("" = unknown)
	Maintenance    bool
	MaintenanceMsg string
	TransitionHeld bool

	OldErr, NewErr error
	OldSt, NewSt   mysql.GTIDState

	StallDisabled bool // the guard itself turned semi-sync off (stall handler)

	ProxyChecked bool
	ProxyOnline  []string
	ProxyWant    string
	ProxyErr     error
}

// EvaluatePreflight returns the ordered checks. The switchover may start only
// when no check failed (Warn checks never fail).
func EvaluatePreflight(in PreflightInput) []Check {
	var out []Check
	add := func(name string, ok bool, detail string) {
		out = append(out, Check{Name: name, OK: ok, Detail: detail})
	}
	warn := func(name string, ok bool, detail string) {
		out = append(out, Check{Name: name, OK: ok, Warn: true, Detail: detail})
	}

	add("no maintenance", !in.Maintenance, orDefault(in.MaintenanceMsg, "maintenance mode is off", in.Maintenance, "maintenance mode is on: "))
	add("no failover or switchover in flight", !in.TransitionHeld, ternary(in.TransitionHeld, "a transition marker is active", "no transition marker"))
	switch {
	case in.CurrentPrimary == "":
		add("old primary is the current writer", false, "writer key unreadable or empty")
	default:
		add("old primary is the current writer", in.CurrentPrimary == in.Old, fmt.Sprintf("writer key names %s, switching %s -> %s", in.CurrentPrimary, in.Old, in.New))
	}
	if in.Old == in.New {
		add("target differs from the old primary", false, in.New+" is already the writer")
	}

	if in.OldErr != nil {
		add("old primary reachable", false, in.OldErr.Error())
	} else {
		add("old primary reachable", true, in.Old)
		add("old primary writable", !in.OldSt.ReadOnly, fmt.Sprintf("read_only=%v", in.OldSt.ReadOnly))
	}
	if in.NewErr != nil {
		add("target reachable", false, in.NewErr.Error())
		return withProxy(out, in)
	}
	add("target reachable", true, in.New)
	add("target is a read-only replica", in.NewSt.ReadOnly && in.NewSt.HasReplicationSource, fmt.Sprintf("read_only=%v, replication source configured=%v", in.NewSt.ReadOnly, in.NewSt.HasReplicationSource))
	add("target IO and SQL threads running", in.NewSt.ReplKnown && in.NewSt.IOThread && in.NewSt.SQLThread && in.NewSt.ReplErr == "",
		fmt.Sprintf("replication %s %s", in.NewSt.ReplState(), in.NewSt.ReplErr))
	add("target lag is 0", in.NewSt.LagSeconds <= 0, fmt.Sprintf("Seconds_Behind_Source=%.0f", in.NewSt.LagSeconds))

	if in.OldErr == nil {
		oldSet, newSet := gtid.Parse(in.OldSt.Executed), gtid.Parse(in.NewSt.Executed)
		gap := gtid.Subtract(oldSet, newSet)
		add("target GTID caught up", gap.Empty(), ternary(gap.Empty(), "target holds every GTID of the old primary", "target is missing "+string(gap)))
		// The two states are read one after the other while the old primary still
		// takes writes, so "target minus old" can be a false positive here. It is
		// advisory; the authoritative errant check runs after the fence.
		extra := gtid.Subtract(newSet, oldSet)
		warn("target has no errant GTIDs (advisory; rechecked after the fence)", extra.Empty(), ternary(extra.Empty(), "none", "target has transactions the old primary lacked at read time: "+string(extra)))

		so := in.OldSt.SemiSync
		switch {
		case in.StallDisabled:
			add("semi-sync acknowledged by the target", false, "semi-sync source was turned off by the stall handler and is not confirmed back on; wait for it to re-enable")
		case !so.Known:
			warn("semi-sync acknowledged by the target", true, "semi-sync state unreadable on the old primary; skipped")
		case !so.SourceEnabled:
			warn("semi-sync acknowledged by the target", true, "semi-sync source is off on the old primary (async cluster); skipped")
		default:
			ok := so.SourceOn && so.SourceClients >= 1 && in.NewSt.SemiSync.Known && in.NewSt.SemiSync.ReplicaOn
			add("semi-sync acknowledged by the target", ok, fmt.Sprintf("source status on=%v, registered clients=%d, target replica status on=%v", so.SourceOn, so.SourceClients, in.NewSt.SemiSync.ReplicaOn))
		}
	}
	return withProxy(out, in)
}

func withProxy(out []Check, in PreflightInput) []Check {
	if !in.ProxyChecked {
		return out
	}
	switch {
	case in.ProxyErr != nil:
		out = append(out, Check{Name: "ProxySQL writer hostgroup readable", Warn: true, Detail: in.ProxyErr.Error()})
	default:
		m := proxysql.WriterMismatch(in.ProxyOnline, in.ProxyWant)
		out = append(out, Check{Name: "ProxySQL shows the old primary as writer", OK: m == "", Warn: true, Detail: ternary(m == "", fmt.Sprintf("writer hostgroup ONLINE=%v", in.ProxyOnline), m)})
	}
	return out
}

// PreflightFailed lists the failed blocking checks.
func PreflightFailed(checks []Check) []Check {
	var bad []Check
	for _, c := range checks {
		if !c.OK && !c.Warn {
			bad = append(bad, c)
		}
	}
	return bad
}

// SwitchoverSteps is the plan the real run follows, for the dry-run print.
func SwitchoverSteps(old, newP string, drain, catchup time.Duration, proxyWait time.Duration, proxyVerify bool) []string {
	s := []string{
		fmt.Sprintf("1. SET GLOBAL super_read_only=ON on %s (old primary); wait up to %s for open transactions, kill the rest", old, drain),
		"2. ProxySQL: take the old primary out of the writer hostgroup (admin mode only; monitor mode writes nothing)",
		fmt.Sprintf("3. read GTID_EXECUTED of %s, wait up to %s for %s to apply every GTID of it", old, catchup, newP),
		"4. gate: the target must hold exactly the old primary's GTIDs (missing or errant -> abort)",
		fmt.Sprintf("5. promote %s: STOP REPLICA, RESET REPLICA ALL, read_only=OFF", newP),
		"6. bump the epoch (CAS) and write the master key / writer service; on failure demote the target and make the old primary writable again",
		fmt.Sprintf("7. re-point %s at %s with the replication user (CHANGE REPLICATION SOURCE, SSL), START REPLICA, wait for it to connect", old, newP),
		"8. restore semi-sync (replica side first, then source on the new primary) when it was on",
	}
	if proxyVerify {
		s = append(s, fmt.Sprintf("9. read-only check: ProxySQL writer hostgroup shows %s within %s (a miss alerts, it never rolls back)", newP, proxyWait))
	} else {
		s = append(s, "9. ProxySQL writer verification skipped (-proxysql-verify is off)")
	}
	s = append(s, "Before step 5 any failure rolls back: the old primary is writable again, the epoch is unchanged.")
	return s
}

// SwitchoverPlan reads the preflight inputs (read-only) and returns the checks.
// Up to three samples are taken because the old primary keeps taking writes:
// the first sample with no blocking failure wins.
func (e *Executor) SwitchoverPlan(ctx context.Context, newArg string) (old, newP string, checks []Check, err error) {
	if e.peers == nil {
		return "", "", nil, fmt.Errorf("peer pool not configured")
	}
	old, newP = mysql.ResolveOldNewPrimary(e.cfg, newArg)
	known := false
	for _, p := range e.cfg.Peers {
		if p == newP {
			known = true
		}
	}
	if !known {
		return "", "", nil, fmt.Errorf("unknown node %q (peers: %s)", newArg, strings.Join(e.cfg.Peers, ", "))
	}
	for i := 0; i < 3; i++ {
		checks = EvaluatePreflight(e.preflightInput(ctx, old, newP))
		if len(PreflightFailed(checks)) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return old, newP, checks, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return old, newP, checks, nil
}

func (e *Executor) preflightInput(ctx context.Context, old, newP string) PreflightInput {
	in := PreflightInput{Old: old, New: newP}
	in.Maintenance, in.MaintenanceMsg = e.maintenance(ctx)
	in.TransitionHeld = e.transitionHeld(ctx)
	if e.store != nil {
		if cur, _, err := e.store.GetPrimary(ctx); err == nil {
			in.CurrentPrimary = cur
		}
		if rec, _ := e.store.GetSemiSync(ctx); rec != nil {
			in.StallDisabled = rec.StallDisabled
		}
	}
	in.OldSt, in.OldErr = e.peers.ReadState(ctx, old)
	in.NewSt, in.NewErr = e.peers.ReadState(ctx, newP)
	if e.proxy != nil && e.proxy.VerifyEnabled() {
		in.ProxyChecked = true
		in.ProxyOnline, in.ProxyErr = e.proxy.WriterHostgroup(ctx)
		in.ProxyWant = e.proxy.ExpectedEndpoint(old)
	}
	return in
}

// PrintSwitchoverPlan writes the preflight and the plan.
func (e *Executor) PrintSwitchoverPlan(w io.Writer, old, newP string, checks []Check, dryRun bool) {
	fmt.Fprintf(w, "switchover %s -> %s (cluster %s)\n\npreflight:\n", old, newP, e.cfg.ClusterName)
	for _, c := range checks {
		mark := "ok  "
		switch {
		case !c.OK && c.Warn:
			mark = "warn"
		case !c.OK:
			mark = "FAIL"
		}
		fmt.Fprintf(w, "  [%s] %s: %s\n", mark, c.Name, c.Detail)
	}
	fmt.Fprintln(w, "\nplan:")
	for _, s := range SwitchoverSteps(old, newP, e.cfg.FenceDrainTimeout, e.cfg.CatchupTimeout(), e.cfg.SwitchoverProxyWait, e.proxy != nil && e.proxy.VerifyEnabled()) {
		fmt.Fprintln(w, "  "+s)
	}
	if bad := PreflightFailed(checks); len(bad) > 0 {
		fmt.Fprintf(w, "\npreflight FAILED (%d blocking check(s)); nothing would be changed.\n", len(bad))
	} else if dryRun {
		fmt.Fprintln(w, "\ndry run: nothing was changed. Run again with -dry-run=false to execute.")
	}
}

// SwitchoverRun is the CLI entry: preflight, print, and (unless dryRun) execute.
func (e *Executor) SwitchoverRun(ctx context.Context, w io.Writer, newArg string, dryRun bool) error {
	old, newP, checks, err := e.SwitchoverPlan(ctx, newArg)
	if err != nil {
		return err
	}
	e.PrintSwitchoverPlan(w, old, newP, checks, dryRun)
	if bad := PreflightFailed(checks); len(bad) > 0 {
		e.actionLog(ctx, 0, "switchover", "preflight failed: "+bad[0].Name+": "+bad[0].Detail, "blocked", "preflight")
		return fmt.Errorf("switchover preflight failed: %s: %s", bad[0].Name, bad[0].Detail)
	}
	if dryRun {
		return nil
	}
	return e.Switchover(ctx, newP, "", "")
}

func ternary(c bool, a, b string) string {
	if c {
		return a
	}
	return b
}

func orDefault(msg, def string, on bool, prefix string) string {
	if on {
		return prefix + msg
	}
	return def
}

// verifyProxyWriter is the post-switchover read-only check: ProxySQL's writer
// hostgroup must show the new primary. In monitor mode ProxySQL moves the
// hostgroup itself from its read_only checks, so this waits up to
// -switchover-proxysql-wait. A miss alerts; it never undoes the promotion.
func (e *Executor) verifyProxyWriter(ctx context.Context, epoch uint64, newPrimary string) {
	if e.proxy == nil || !e.proxy.VerifyEnabled() {
		e.actionLog(ctx, epoch, "switchover", "ProxySQL writer verification not enabled (-proxysql-verify)", "ok", "proxysql_verify")
		return
	}
	want := e.proxy.ExpectedEndpoint(newPrimary)
	deadline := time.Now().Add(e.cfg.SwitchoverProxyWait)
	var mismatch string
	for {
		online, err := e.proxy.WriterHostgroup(ctx)
		if err != nil {
			mismatch = "ProxySQL writer hostgroup unreadable: " + err.Error()
		} else if want == "" {
			e.actionLog(ctx, epoch, "switchover", "no ProxySQL endpoint known for "+newPrimary+"; writer check skipped", "warn", "proxysql_verify")
			return
		} else if mismatch = proxysql.WriterMismatch(online, want); mismatch == "" {
			e.actionLog(ctx, epoch, "switchover", "ProxySQL writer hostgroup shows "+want, "ok", "proxysql_verify")
			return
		}
		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
	msg := fmt.Sprintf("switchover to %s done, but %s after %s", newPrimary, mismatch, e.cfg.SwitchoverProxyWait)
	e.actionLog(ctx, epoch, "switchover", msg, "warn", "proxysql_verify")
	e.Alert(ctx, "switchover-proxysql", msg, true)
}
