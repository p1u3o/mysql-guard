package guard

import (
	"errors"
	"strings"
	"testing"

	"mysql-guard/internal/config"
	"mysql-guard/internal/vote"
)

type fakeHealth struct{ st, out []string }

func (f *fakeHealth) UpdateGuard(st, out string) error {
	f.st, f.out = append(f.st, st), append(f.out, out)
	return nil
}

func TestGuardCheckStatus(t *testing.T) {
	cases := []struct {
		name string
		in   tickHealth
		want string
	}{
		{"db healthy", tickHealth{role: config.RoleDB, probe: vote.ProbeOK, primary: "kv01", epoch: 1}, "passing"},
		{"witness healthy", tickHealth{role: config.RoleWitness}, "passing"},
		{"local mysql down", tickHealth{role: config.RoleDB, probe: vote.ProbeFailed}, "critical"},
		{"local mysql refused", tickHealth{role: config.RoleDB, probe: vote.ProbeRefused}, "critical"},
		{"consul down", tickHealth{role: config.RoleWitness, consulErr: errors.New("eof")}, "critical"},
		{"repl unknown", tickHealth{role: config.RoleDB, probe: vote.ProbeOK, replUnknown: true, replErr: "denied"}, "warning"},
		{"slow", tickHealth{role: config.RoleDB, probe: vote.ProbeSlow}, "warning"},
	}
	for _, c := range cases {
		if got, _ := guardCheckStatus(c.in); got != c.want {
			t.Errorf("%s: got %s want %s", c.name, got, c.want)
		}
	}
}

func TestReportHealthEveryTick(t *testing.T) {
	f := &fakeHealth{}
	r := &Runtime{cfg: &config.Config{}}
	r.reportHealth(tickHealth{}) // no reporter: no panic
	r.SetHealthReporter(f)
	for i := 0; i < 3; i++ {
		r.reportHealth(tickHealth{role: config.RoleDB, probe: vote.ProbeOK, leader: true, primary: "kv01", epoch: 1})
	}
	if len(f.st) != 3 || f.st[2] != "passing" || !strings.Contains(f.out[2], "leader=true") {
		t.Fatalf("want a passing update per tick, got %v %v", f.st, f.out)
	}
}
