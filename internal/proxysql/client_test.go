package proxysql

import (
	"context"
	"testing"

	"mysql-guard/internal/config"
)

func TestPlanPromote(t *testing.T) {
	steps := PlanPromote(AdminAction{
		WriterGroup: 10, ReaderGroup: 11,
		NewEndpoint: Endpoint{Host: "db-b.example", Port: 3306},
		OldEndpoint: Endpoint{Host: "db-a.example", Port: 3306},
	})
	if len(steps) < 3 {
		t.Fatal(steps)
	}
}

func TestPlanFence(t *testing.T) {
	steps := PlanFence(AdminAction{ReaderGroup: 11, OldEndpoint: Endpoint{Host: "db-a.example", Port: 3306}})
	if len(steps) != 2 {
		t.Fatalf("%v", steps)
	}
}

func TestDisabledClientNoop(t *testing.T) {
	c, err := NewClient(config.ProxySQLSettings{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ApplyPromote(context.Background(), "db-b", "db-a"); err != nil {
		t.Fatal(err)
	}
}
