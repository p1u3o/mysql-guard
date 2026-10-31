package actions

import (
	"errors"
	"strings"
	"testing"

	"mysql-guard/internal/mysql"
)

func healthyInput() PreflightInput {
	return PreflightInput{
		Old: "db-a", New: "db-b", CurrentPrimary: "db-a",
		OldSt: mysql.GTIDState{Executed: "uuid:1-100", SemiSync: mysql.SemiSync{Known: true, Installed: true, SourceEnabled: true, SourceOn: true, SourceClients: 1}},
		NewSt: mysql.GTIDState{Executed: "uuid:1-100", ReadOnly: true, HasReplicationSource: true, ReplKnown: true, IOThread: true, SQLThread: true,
			SemiSync: mysql.SemiSync{Known: true, Installed: true, ReplicaEnabled: true, ReplicaOn: true}},
	}
}

func failedNames(in PreflightInput) string {
	var n []string
	for _, c := range PreflightFailed(EvaluatePreflight(in)) {
		n = append(n, c.Name)
	}
	return strings.Join(n, "; ")
}

func TestPreflightHealthyPasses(t *testing.T) {
	if f := failedNames(healthyInput()); f != "" {
		t.Fatalf("healthy input failed: %s", f)
	}
}

func TestPreflightBlocks(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*PreflightInput)
		want string
	}{
		{"maintenance", func(in *PreflightInput) { in.Maintenance = true; in.MaintenanceMsg = "patching" }, "no maintenance"},
		{"transition", func(in *PreflightInput) { in.TransitionHeld = true }, "no failover or switchover in flight"},
		{"writer key elsewhere", func(in *PreflightInput) { in.CurrentPrimary = "db-b" }, "old primary is the current writer"},
		{"writer key unknown", func(in *PreflightInput) { in.CurrentPrimary = "" }, "old primary is the current writer"},
		{"target down", func(in *PreflightInput) { in.NewErr = errors.New("refused") }, "target reachable"},
		{"old down", func(in *PreflightInput) { in.OldErr = errors.New("refused") }, "old primary reachable"},
		{"target writable", func(in *PreflightInput) { in.NewSt.ReadOnly = false }, "target is a read-only replica"},
		{"io stopped", func(in *PreflightInput) { in.NewSt.IOThread = false }, "target IO and SQL threads running"},
		{"repl error", func(in *PreflightInput) { in.NewSt.ReplErr = "1236" }, "target IO and SQL threads running"},
		{"lag", func(in *PreflightInput) { in.NewSt.LagSeconds = 2 }, "target lag is 0"},
		{"gtid behind", func(in *PreflightInput) { in.NewSt.Executed = "uuid:1-90" }, "target GTID caught up"},
		{"semi source fell back", func(in *PreflightInput) { in.OldSt.SemiSync.SourceOn = false }, "semi-sync acknowledged by the target"},
		{"no semi client", func(in *PreflightInput) { in.OldSt.SemiSync.SourceClients = 0 }, "semi-sync acknowledged by the target"},
		{"target replica semi off", func(in *PreflightInput) { in.NewSt.SemiSync.ReplicaOn = false }, "semi-sync acknowledged by the target"},
		{"stall disabled", func(in *PreflightInput) { in.StallDisabled = true; in.OldSt.SemiSync.SourceEnabled = false }, "semi-sync acknowledged by the target"},
		{"same node", func(in *PreflightInput) { in.New = "db-a" }, "target differs from the old primary"},
	}
	for _, c := range cases {
		in := healthyInput()
		c.mut(&in)
		if f := failedNames(in); !strings.Contains(f, c.want) {
			t.Errorf("%s: failed=%q, want %q", c.name, f, c.want)
		}
	}
}

func TestPreflightErrantIsAdvisory(t *testing.T) {
	in := healthyInput()
	in.NewSt.Executed = "uuid:1-101" // applied a write after the old primary was read
	checks := EvaluatePreflight(in)
	if len(PreflightFailed(checks)) != 0 {
		t.Fatalf("a target ahead at read time must not block: %v", PreflightFailed(checks))
	}
	warned := false
	for _, c := range checks {
		if c.Warn && !c.OK && strings.Contains(c.Name, "errant") {
			warned = true
		}
	}
	if !warned {
		t.Fatal("expected an errant warning")
	}
}

func TestPreflightAsyncClusterSkipsSemiSync(t *testing.T) {
	in := healthyInput()
	in.OldSt.SemiSync = mysql.SemiSync{Known: true, Installed: true}
	in.NewSt.SemiSync = mysql.SemiSync{}
	if f := failedNames(in); f != "" {
		t.Fatalf("async cluster blocked: %s", f)
	}
}

func TestPreflightProxyIsAdvisory(t *testing.T) {
	in := healthyInput()
	in.ProxyChecked, in.ProxyWant, in.ProxyOnline = true, "10.0.0.1:3306", []string{"10.0.0.2:3306"}
	checks := EvaluatePreflight(in)
	if len(PreflightFailed(checks)) != 0 {
		t.Fatal("a ProxySQL mismatch must not block")
	}
	last := checks[len(checks)-1]
	if last.OK || !last.Warn {
		t.Fatalf("want a warning, got %+v", last)
	}
}

func TestSwitchoverStepsMentionRollbackAndVerify(t *testing.T) {
	s := strings.Join(SwitchoverSteps("a", "b", 0, 0, 0, true), "\n")
	for _, w := range []string{"super_read_only", "rolls back", "ProxySQL writer hostgroup shows b"} {
		if !strings.Contains(s, w) {
			t.Errorf("plan lacks %q:\n%s", w, s)
		}
	}
}
