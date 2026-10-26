package proxysql

import (
	"strings"
	"testing"
)

func TestResolveEndpoint(t *testing.T) {
	ep := ResolveEndpoint("db-a", map[string]string{"db-a": "10.0.0.1:3307"})
	if ep.Host != "10.0.0.1" || ep.Port != 3307 {
		t.Fatalf("%+v", ep)
	}
	ep = ResolveEndpoint("db-b", nil)
	if ep.Host != "db-b" || ep.Port != 3306 {
		t.Fatalf("%+v", ep)
	}
}

func TestPlanPromoteMatchesHostPort(t *testing.T) {
	steps := PlanPromote(AdminAction{
		WriterGroup: 10, ReaderGroup: 11,
		NewEndpoint: Endpoint{Host: "10.0.0.2", Port: 3306},
		OldEndpoint: Endpoint{Host: "10.0.0.1", Port: 3306},
	})
	if !strings.Contains(steps[0], "hostname='10.0.0.2'") || !strings.Contains(steps[0], "port=3306") {
		t.Fatalf("%v", steps)
	}
}
