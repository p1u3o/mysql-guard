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
