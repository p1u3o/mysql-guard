package actions

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"mysql-guard/internal/config"
	"mysql-guard/internal/gtid"
	"mysql-guard/internal/mysql"
	"mysql-guard/internal/rejoin"
)

func TestRejoinErrantBlocked(t *testing.T) {
	cfg := &config.Config{Mode: config.ModeAct, NodeName: "db-a", ClusterName: "c", Peers: []string{"db-a", "db-b"}, ActionLogMax: 100}
	ex := NewExecutor(cfg, slog.New(slog.NewTextHandler(os.Stderr, nil)), &mysql.Prober{}, nil, nil, nil, nil)
	u := gtid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100")
	errant := gtid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-110")
	plan, err := ex.Rejoin(context.Background(), "db-b", "db-a", errant, u, true)
	if err == nil || plan.Outcome != rejoin.OutcomeBlockedErrant {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
}
