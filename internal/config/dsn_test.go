package config

import "testing"

func TestBuildDSNFromPasswordFile(t *testing.T) {
	c := &Config{
		NodeName:       "db-a",
		ClusterName:    "test",
		Peers:          []string{"db-a", "db-b"},
		MySQLAdminUser: "guard",
		MySQLAdminPass: "s3cret",
		MySQLHost:      "127.0.0.1",
		MySQLPort:      3306,
	}
	if err := c.FinalizeMySQLDSN(); err != nil {
		t.Fatal(err)
	}
	if c.MySQLDSN == "" || !dsnContainsPassword(c.MySQLDSN) {
		t.Fatalf("dsn=%q", c.MySQLDSN)
	}
}

func TestDSNOverrideRejectsEmbeddedPasswordWithFile(t *testing.T) {
	c := &Config{
		MySQLDSN:           "guard:pass@tcp(127.0.0.1:3306)/",
		MySQLAdminPassFile: "/tmp/pw",
		MySQLAdminPass:     "x",
	}
	if err := c.FinalizeMySQLDSN(); err == nil {
		t.Fatal("expected error")
	}
}
