package guard

import (
	"errors"
	"testing"

	"mysql-guard/internal/config"
	"mysql-guard/internal/mysql"
	"mysql-guard/internal/proxysql"
)

func TestCandidateUsableRequiresKnownReplication(t *testing.T) {
	if candidateUsable(peerObs{st: mysql.GTIDState{ReplKnown: false, ReplErr: "denied"}}) {
		t.Fatal("unknown replication state must never be a failover candidate")
	}
	if candidateUsable(peerObs{err: errors.New("down"), st: mysql.GTIDState{ReplKnown: true}}) {
		t.Fatal("unreachable node must not be a candidate")
	}
	if !candidateUsable(peerObs{st: mysql.GTIDState{ReplKnown: true}}) {
		t.Fatal("reachable node with known state must be a candidate")
	}
}

func TestProxySQLHiddenWhenNotConfigured(t *testing.T) {
	px, _ := proxysql.NewClient(config.ProxySQLSettings{})
	r := &Runtime{cfg: &config.Config{}, proxy: px}
	if r.proxySQLConfigured() {
		t.Fatal("no admin DSN: proxysql must count as not configured")
	}
	r.proxy = nil
	if r.proxySQLConfigured() {
		t.Fatal("nil client: not configured")
	}
	on, _ := proxysql.NewClient(config.ProxySQLSettings{AdminEnabled: true})
	r.proxy = on
	if !r.proxySQLConfigured() {
		t.Fatal("admin enabled: configured")
	}
}
