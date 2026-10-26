package guard

import (
	"testing"
	"time"

	"mysql-guard/internal/config"
	"mysql-guard/internal/consul"
)

func TestWitnessSkipsMySQLAndReplicationChecks(t *testing.T) {
	w := ChecksFor(config.RoleWitness)
	if w.MySQL || w.Replication {
		t.Fatalf("witness must not run MySQL/replication checks: %+v", w)
	}
	d := ChecksFor(config.RoleDB)
	if !d.MySQL || !d.Replication {
		t.Fatalf("db guard must run all checks: %+v", d)
	}
}

func TestCountDeadVotes(t *testing.T) {
	now := time.Now()
	guards := []consul.GuardInstance{
		{Node: "db-a", Role: "db", PrimaryView: "db-a", PrimaryOK: false, LastHeartbeat: now},
		{Node: "db-b", Role: "db", PrimaryView: "db-a", PrimaryOK: false, LastHeartbeat: now},
		{Node: "witness-1", Role: "witness", PrimaryView: "", PrimaryOK: false, LastHeartbeat: now},
	}
	if v := CountDeadVotes("db-b", false, "db-a", guards, now, 10*time.Second); v != 2 {
		t.Fatalf("expected 2 votes, got %d", v)
	}
	if v := CountDeadVotes("db-b", true, "db-a", guards, now, 10*time.Second); v != 1 {
		t.Fatalf("expected 1 vote, got %d", v)
	}
	guards[0].LastHeartbeat = now.Add(-time.Minute)
	if v := CountDeadVotes("db-b", false, "db-a", guards, now, 10*time.Second); v != 1 {
		t.Fatalf("stale report must not count, got %d", v)
	}
	guards[0].LastHeartbeat = now
	guards[0].PrimaryView = "db-b"
	if v := CountDeadVotes("db-b", false, "db-a", guards, now, 10*time.Second); v != 1 {
		t.Fatalf("report about another primary must not count, got %d", v)
	}
}
