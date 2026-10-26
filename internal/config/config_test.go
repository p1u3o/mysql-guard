package config

import "testing"

func TestClusterRequired(t *testing.T) {
	_, err := Load([]string{"--node=db-a", "--peers=db-a,db-b"})
	if err == nil {
		t.Fatal("expected cluster required")
	}
}

func TestPeersRequired(t *testing.T) {
	_, err := Load([]string{"--node=db-a", "--cluster=test"})
	if err == nil {
		t.Fatal("expected peers required")
	}
}

func TestMinimalValid(t *testing.T) {
	cfg, err := Load([]string{"--node=db-a", "--cluster=test", "--peers=db-a,db-b"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClusterName != "test" || len(cfg.Peers) != 2 {
		t.Fatalf("%+v", cfg)
	}
}

func TestReplCredentialsFallbackAndEnvPort(t *testing.T) {
	c := &Config{MySQLAdminUser: "guard", MySQLAdminPass: "g"}
	if u, p := c.ReplCredentials(); u != "guard" || p != "g" {
		t.Fatalf("fallback got %s/%s", u, p)
	}
	c.MySQLReplUser, c.MySQLReplPass = "repl", "r"
	if u, p := c.ReplCredentials(); u != "repl" || p != "r" {
		t.Fatalf("repl got %s/%s", u, p)
	}
	t.Setenv("MYSQL_PORT", "18401")
	if envInt("MYSQL_PORT", 3306) != 18401 {
		t.Fatal("MYSQL_PORT env not honoured")
	}
	t.Setenv("MYSQL_PORT", "")
	if envInt("MYSQL_PORT", 3306) != 3306 {
		t.Fatal("default port")
	}
}
