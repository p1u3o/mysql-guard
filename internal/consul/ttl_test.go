package consul

import (
	"errors"
	"testing"

	capi "github.com/hashicorp/consul/api"
)

type fakeTTL struct {
	calls []string
	err   error
}

func (f *fakeTTL) UpdateTTL(id, out, st string) error {
	f.calls = append(f.calls, id+"|"+st)
	return f.err
}

func TestUpdateGuardUsesOwnCheckAndRetries(t *testing.T) {
	f := &fakeTTL{err: errors.New("403")}
	a := &AgentChecks{serviceID: "svc-kv01", checkID: "svc-kv01:guard", ttl: f}
	if a.UpdateGuard(capi.HealthPassing, "ok") == nil {
		t.Fatal("want error")
	}
	f.err = nil
	a.PassTTL("ok")
	_ = a.UpdateGuard(capi.HealthWarning, "slow")
	want := []string{"svc-kv01:guard|passing", "svc-kv01:guard|passing", "svc-kv01:guard|warning"}
	if len(f.calls) != 3 {
		t.Fatalf("not retried after error (v0.2.2 disabled forever): %v", f.calls)
	}
	for i := range want {
		if f.calls[i] != want[i] {
			t.Fatalf("calls=%v want %v", f.calls, want)
		}
	}
}

func TestAlertsDoNotTouchGuardCheck(t *testing.T) {
	f := &fakeTTL{}
	a := &AgentChecks{serviceID: "svc", checkID: "svc:guard", ttl: f}
	_ = a.SetCheckStatus(nil, "guard-repl_io_stopped", "critical", "x")
	if len(f.calls) != 0 {
		t.Fatalf("alert overwrote guard TTL check: %v", f.calls)
	}
	b := &AgentChecks{ttl: f} // no service id -> no-op
	b.PassTTL("ok")
	if len(f.calls) != 0 {
		t.Fatal("no service id must not update")
	}
}
