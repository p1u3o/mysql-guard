package actions

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"mysql-guard/internal/config"
	"mysql-guard/internal/mysql"
)

func TestSemiStallRefusedOutsideActMode(t *testing.T) {
	for _, mode := range []config.Mode{config.ModeObserve, config.ModeFence} {
		cfg := &config.Config{Mode: mode, NodeName: "db-a", ClusterName: "c", Peers: []string{"db-a", "db-b"}, ActionLogMax: 100}
		ex := NewExecutor(cfg, slog.New(slog.NewTextHandler(os.Stderr, nil)), &mysql.Prober{}, nil, nil, nil, nil)
		// nil pool and store would panic if any SQL or KV call were made
		if err := ex.SemiStallDisable(context.Background(), "db-a", 1, "x"); !errors.Is(err, ErrSemiStallSkipped) {
			t.Fatalf("%s disable: %v", mode, err)
		}
		if err := ex.SemiStallReenable(context.Background(), "db-a", 1, "x"); !errors.Is(err, ErrSemiStallSkipped) {
			t.Fatalf("%s reenable: %v", mode, err)
		}
	}
}

func TestSemiStallRefusedWithoutPool(t *testing.T) {
	cfg := &config.Config{Mode: config.ModeAct, NodeName: "db-a", ClusterName: "c", Peers: []string{"db-a", "db-b"}, ActionLogMax: 100}
	ex := NewExecutor(cfg, slog.New(slog.NewTextHandler(os.Stderr, nil)), &mysql.Prober{}, nil, nil, nil, nil)
	if err := ex.SemiStallDisable(context.Background(), "db-a", 1, "x"); !errors.Is(err, ErrSemiStallSkipped) {
		t.Fatalf("%v", err)
	}
}
