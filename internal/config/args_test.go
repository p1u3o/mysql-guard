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
