package mysql

import (
	"testing"

	"mysql-guard/internal/config"
)

func TestBuildPeerDSN(t *testing.T) {
	cfg := &config.Config{
		NodeName:       "db-a",
		MySQLAdminUser: "guard",
		MySQLAdminPass: "secret",
		MySQLHost:      "127.0.0.1",
		MySQLPort:      3306,
		MySQLPeerEndpoints: map[string]string{
			"db-a": "10.0.0.1:3306",
			"db-b": "10.0.0.2:3306",
		},
	}
	dsn, err := BuildPeerDSN(cfg, "db-b")
	if err != nil || dsn == "" {
		t.Fatalf("err=%v dsn=%q", err, dsn)
	}
}

func TestResolveOldNewPrimary(t *testing.T) {
	cfg := &config.Config{Peers: []string{"db-a", "db-b"}}
	old, new := ResolveOldNewPrimary(cfg, "db-b")
	if old != "db-a" || new != "db-b" {
		t.Fatalf("%s %s", old, new)
	}
}
