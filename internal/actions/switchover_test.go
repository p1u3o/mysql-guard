package actions

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"mysql-guard/internal/config"
	"mysql-guard/internal/gtid"
	"mysql-guard/internal/mysql"
)

func TestSwitchoverObserveSkipsSQL(t *testing.T) {
	cfg := &config.Config{Mode: config.ModeObserve, NodeName: "db-a", ClusterName: "c", Peers: []string{"db-a", "db-b"}, ActionLogMax: 100}
	ex := NewExecutor(cfg, slog.New(slog.NewTextHandler(os.Stderr, nil)), &mysql.Prober{}, nil, nil, nil, nil)
	u := gtid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10")
	// v0.3.0: observe refuses (error) and still issues no SQL (nil pool would panic otherwise)
	if err := ex.Switchover(context.Background(), "db-b", u, u); err == nil {
		t.Fatal("expected observe-mode refusal")
	}
}

func TestSwitchoverGTIDMismatch(t *testing.T) {
	cfg := &config.Config{Mode: config.ModeAct, NodeName: "db-a", ClusterName: "c", Peers: []string{"db-a", "db-b"}, ActionLogMax: 100}
	ex := NewExecutor(cfg, slog.New(slog.NewTextHandler(os.Stderr, nil)), &mysql.Prober{}, nil, nil, nil, nil)
	a := gtid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10")
	b := gtid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-11")
	if err := ex.Switchover(context.Background(), "db-b", a, b); err == nil {
		t.Fatal("expected mismatch error")
	}
}
