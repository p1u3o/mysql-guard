package actions

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"mysql-guard/internal/config"
	"mysql-guard/internal/gtid"
	"mysql-guard/internal/mysql"
	"mysql-guard/internal/notify"
	"mysql-guard/internal/proxysql"
)

func TestPromoteBlockedErrant(t *testing.T) {
	cfg := &config.Config{Mode: config.ModeAct, NodeName: "db-b", Role: config.RoleDB, Promotable: true, ClusterName: "test", Peers: []string{"db-a", "db-b"}}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	my, _ := mysql.NewProber("", mysql.ProbeConfig{})
	px, _ := proxysql.NewClient(config.ProxySQLSettings{})
	ex := NewExecutor(cfg, log, my, nil, px, nil, &notify.Fanout{Ntfy: &notify.LogSender{}})
	u := gtid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100")
	errant := gtid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-110")
	err := ex.Promote(context.Background(), "db-b", "db-a", errant, u, 1)
	if err == nil {
		t.Fatal("expected errant block")
	}
}
