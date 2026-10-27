package consul

import (
	"strings"
	"testing"
	"time"
)

func TestMemberRegistration(t *testing.T) {
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	reg, err := MemberRegistration("mysql-prod", "prod", "mysql01", "10.0.0.1:3306", MemberState{Role: "reader", Status: "passing", Reason: "ok"}, 7, "mysql02", now)
	if err != nil {
		t.Fatal(err)
	}
	if reg.Node != "mysql-prod-mysql01-vnode" || reg.Service.ID != "mysql-prod-mysql01" || reg.Service.Service != "mysql-prod" || reg.Service.Port != 3306 || reg.Service.Address != "10.0.0.1" {
		t.Fatalf("%+v %+v", reg, reg.Service)
	}
	if len(reg.Service.Tags) != 1 || reg.Service.Tags[0] != "reader" {
		t.Fatal(reg.Service.Tags)
	}
	m := reg.Service.Meta
	if m["cluster"] != "prod" || m["node"] != "mysql01" || m["epoch"] != "7" || m["role"] != "reader" {
		t.Fatal(m)
	}
	c := reg.Checks[0]
	if c.CheckID != "mysql-prod:mysql01:mysql" || c.Status != "passing" || c.ServiceID != "mysql-prod-mysql01" || !strings.HasPrefix(c.Output, "as-of=2026-10-27T12:00:00Z epoch=7 by=mysql02") {
		t.Fatalf("%+v", c)
	}
	// unknown role -> fenced/critical; fenced is always critical
	reg, _ = MemberRegistration("s", "c", "n", "h:1", MemberState{Role: "bogus", Status: "passing"}, 1, "x", now)
	if reg.Service.Tags[0] != "fenced" || reg.Checks[0].Status != "critical" {
		t.Fatal("fenced must be critical")
	}
	if _, err := MemberRegistration("s", "c", "n", "nohostport", MemberState{}, 1, "x", now); err == nil {
		t.Fatal("bad endpoint")
	}
}
