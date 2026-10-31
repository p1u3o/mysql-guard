package config

import "testing"

func TestSplitFlagsAfterTarget(t *testing.T) {
	_, pos := SplitFlagsAndPositionals([]string{"db-b", "--cluster=prod", "--peers=db-a,db-b"})
	if len(pos) != 1 || pos[0] != "db-b" {
		t.Fatalf("pos=%v", pos)
	}
}

func TestSplitFlagsBeforeTarget(t *testing.T) {
	_, pos := SplitFlagsAndPositionals([]string{"--cluster=prod", "--node=db-a", "--peers=db-a,db-b", "db-b"})
	if len(pos) != 1 || pos[0] != "db-b" {
		t.Fatalf("pos=%v", pos)
	}
	cfg, target, err := LoadSubcommand([]string{"--cluster=prod", "--node=db-a", "--peers=db-a,db-b", "db-b"})
	if err != nil {
		t.Fatal(err)
	}
	if target != "db-b" || cfg.ClusterName != "prod" {
		t.Fatalf("target=%q cluster=%q", target, cfg.ClusterName)
	}
}

func TestSubcommandClusterRequired(t *testing.T) {
	_, _, err := LoadSubcommand([]string{"db-b", "--node=db-a", "--peers=db-a,db-b"})
	if err == nil {
		t.Fatal("expected cluster required")
	}
}

func TestSubcommandBoolFlagsDoNotSwallowTarget(t *testing.T) {
	for _, b := range []string{"-mysql-tls", "--proxysql-verify", "--proxysql-admin-enable", "-foreign-write-alert"} {
		cfg, target, err := LoadSubcommand([]string{"--cluster=prod", "--node=db-a", "--peers=db-a,db-b", b, "db-b"})
		if err != nil {
			t.Fatalf("%s: %v", b, err)
		}
		if target != "db-b" {
			t.Fatalf("%s swallowed the target: target=%q", b, target)
		}
		_ = cfg
	}
	cfg, target, err := LoadSubcommand([]string{"rejoin-src", "-mysql-tls", "--proxysql-verify", "--cluster=prod", "--node=db-a", "--peers=db-a,db-b"})
	if err != nil || target != "rejoin-src" || !cfg.MySQLTLS || !cfg.ProxySQLVerify {
		t.Fatalf("target=%q tls=%v verify=%v err=%v", target, cfg != nil && cfg.MySQLTLS, cfg != nil && cfg.ProxySQLVerify, err)
	}
}

func TestSubcommandBoolExplicitFalse(t *testing.T) {
	cfg, target, err := LoadSubcommand([]string{"--cluster=prod", "--node=db-a", "--peers=db-a,db-b", "--foreign-write-alert=false", "db-b"})
	if err != nil || target != "db-b" || cfg.ForeignWriteAlert {
		t.Fatalf("target=%q fwa=%v err=%v", target, cfg.ForeignWriteAlert, err)
	}
}

func TestSubcommandValueFlagStillTakesValue(t *testing.T) {
	cfg, target, err := LoadSubcommand([]string{"--cluster", "prod", "--node", "db-a", "--peers", "db-a,db-b", "db-b"})
	if err != nil || target != "db-b" || cfg.ClusterName != "prod" || cfg.NodeName != "db-a" {
		t.Fatalf("target=%q err=%v", target, err)
	}
}

func TestIsBoolFlag(t *testing.T) {
	for n, want := range map[string]bool{"mysql-tls": true, "proxysql-verify": true, "proxysql-admin-enable": true, "foreign-write-alert": true, "cluster": false, "interval": false, "nope": false} {
		if got := isBoolFlag(n); got != want {
			t.Errorf("isBoolFlag(%q)=%v want %v", n, got, want)
		}
	}
}

func TestSwitchoverToAndDryRunDefault(t *testing.T) {
	base := []string{"--cluster=prod", "--node=db-a", "--peers=db-a,db-b"}
	cfg, target, err := LoadSubcommand(append([]string{"--to", "db-b"}, base...))
	if err != nil || target != "" || cfg.SwitchoverTo != "db-b" || !cfg.SwitchoverDryRun {
		t.Fatalf("to=%q target=%q dry=%v err=%v", cfg.SwitchoverTo, target, cfg.SwitchoverDryRun, err)
	}
	cfg, _, err = LoadSubcommand(append(base, "--to=db-b", "--dry-run=false"))
	if err != nil || cfg.SwitchoverTo != "db-b" || cfg.SwitchoverDryRun {
		t.Fatalf("to=%q dry=%v err=%v", cfg.SwitchoverTo, cfg.SwitchoverDryRun, err)
	}
	// a bare -dry-run must not swallow the positional target
	cfg, target, err = LoadSubcommand(append([]string{"--dry-run", "db-b"}, base...))
	if err != nil || target != "db-b" || !cfg.SwitchoverDryRun {
		t.Fatalf("target=%q dry=%v err=%v", target, cfg.SwitchoverDryRun, err)
	}
}

func TestSemiStallFlagsDefaultOff(t *testing.T) {
	cfg, _, err := LoadSubcommand([]string{"--cluster=prod", "--node=db-a", "--peers=db-a,db-b"})
	if err != nil || cfg.SemiSyncStall || cfg.SemiSyncStallProbes != 3 {
		t.Fatalf("stall=%v probes=%d err=%v", cfg.SemiSyncStall, cfg.SemiSyncStallProbes, err)
	}
}
