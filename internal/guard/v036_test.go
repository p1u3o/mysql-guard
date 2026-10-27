package guard

import (
	"testing"
	"time"

	"mysql-guard/internal/consul"

	capi "github.com/hashicorp/consul/api"
)

func q(t *testing.T, in QuorumInput) Quorum {
	t.Helper()
	if in.Now.IsZero() {
		in.Now = time.Unix(1000, 0)
	}
	if in.MaxAge == 0 {
		in.MaxAge = 7 * time.Second
	}
	return CountQuorum(in)
}

func g(node, role string, age time.Duration, view string) consul.GuardInstance {
	return consul.GuardInstance{Node: node, Role: role, LastHeartbeat: time.Unix(1000, 0).Add(-age), PrimaryView: view, PrimaryOK: true}
}

func TestQuorumScenarios(t *testing.T) {
	const P, R, W = "kv01", "kv02", "prod02"
	witness := func(probe string) consul.GuardInstance {
		x := g(W, "witness", time.Second, P)
		x.PrimaryProbe = probe
		return x
	}
	primaryGuard := func(ok bool) consul.GuardInstance {
		x := g(P, "db", time.Second, P)
		x.PrimaryOK = ok
		return x
	}
	staleGuard := g(P, "db", time.Minute, P)
	cases := []struct {
		name   string
		in     QuorumInput
		votes  int
		voters string
	}{
		{"D1 whole host lost: replica probe + witness + io", QuorumInput{Self: R, SelfPrimaryOK: false, SelfIOLost: true, Primary: P, Guards: []consul.GuardInstance{staleGuard, witness("dead")}}, 3, "kv02:io,kv02:probe,prod02:witness"},
		{"D1 host lost, io not yet lost", QuorumInput{Self: R, SelfPrimaryOK: false, Primary: P, Guards: []consul.GuardInstance{staleGuard, witness("dead")}}, 2, "kv02:probe,prod02:witness"},
		{"host lost, witness without creds: probe + io", QuorumInput{Self: R, SelfPrimaryOK: false, SelfIOLost: true, Primary: P, Guards: []consul.GuardInstance{staleGuard, witness("")}}, 2, "kv02:io,kv02:probe"},
		{"D3 replica isolated from primary: primary guard fresh blocks io", QuorumInput{Self: R, SelfPrimaryOK: false, SelfIOLost: true, Primary: P, Guards: []consul.GuardInstance{primaryGuard(true), witness("ok")}}, 1, "kv02:probe"},
		{"primary guard crashed only, witness sees ok: io vetoed", QuorumInput{Self: R, SelfPrimaryOK: false, SelfIOLost: true, Primary: P, Guards: []consul.GuardInstance{staleGuard, witness("ok")}}, 1, "kv02:probe"},
		{"D4 witness lost + mysqld killed", QuorumInput{Self: R, SelfPrimaryOK: false, Primary: P, Guards: []consul.GuardInstance{primaryGuard(false), g(W, "witness", time.Minute, P)}}, 2, "kv01:probe,kv02:probe"},
		{"witness alone", QuorumInput{Self: R, SelfPrimaryOK: true, Primary: P, Guards: []consul.GuardInstance{primaryGuard(true), witness("dead")}}, 1, "prod02:witness"},
		{"stale witness ignored", QuorumInput{Self: R, SelfPrimaryOK: false, Primary: P, Guards: []consul.GuardInstance{primaryGuard(true), func() consul.GuardInstance { x := witness("dead"); x.LastHeartbeat = time.Unix(0, 0); return x }()}}, 1, "kv02:probe"},
		{"witness about another primary ignored", QuorumInput{Self: R, SelfPrimaryOK: false, Primary: P, Guards: []consul.GuardInstance{func() consul.GuardInstance { x := witness("dead"); x.PrimaryView = R; return x }()}}, 1, "kv02:probe"},
		{"leader is primary guard, mysqld down", QuorumInput{Self: P, SelfPrimaryOK: false, Primary: P, Guards: []consul.GuardInstance{func() consul.GuardInstance {
			x := g(R, "db", time.Second, P)
			x.PrimaryOK = false
			x.ReplicaIOLost = true
			return x
		}()}}, 2, "kv01:probe,kv02:probe"},
		{"healthy", QuorumInput{Self: R, SelfPrimaryOK: true, Primary: P, Guards: []consul.GuardInstance{primaryGuard(true), witness("ok")}}, 0, "no votes"},
	}
	for _, c := range cases {
		got := q(t, c.in)
		if got.Votes != c.votes || got.String() != c.voters {
			t.Errorf("%s: got %d %q want %d %q", c.name, got.Votes, got.String(), c.votes, c.voters)
		}
	}
}

func TestSemiSyncGate(t *testing.T) {
	if ok, _ := SemiSyncGateDecision(false, false, "", 0); !ok {
		t.Fatal("no sample must not block")
	}
	if ok, _ := SemiSyncGateDecision(true, false, "off", time.Second); !ok {
		t.Fatal("semi-sync off must not block")
	}
	if ok, n := SemiSyncGateDecision(true, true, "sync", 5*time.Second); !ok || n != "semi-sync at loss: sync (last sample 5s before)" {
		t.Fatalf("sync: %v %q", ok, n)
	}
	if ok, _ := SemiSyncGateDecision(true, true, "async", time.Second); ok {
		t.Fatal("async with semi-sync enabled must block")
	}
}

func TestMemberDesired(t *testing.T) {
	ok := MemberInput{Node: "kv02", Reachable: true, ReplKnown: true, ReadOnly: true, IOThread: true, SQLThread: true, LagSeconds: 1}
	chk := func(name string, in MemberInput, role, status string, imm bool) {
		t.Helper()
		s, i := MemberDesired("kv01", in, 30)
		if s.Role != role || s.Status != status || i != imm {
			t.Errorf("%s: got %s/%s/%v (%s)", name, s.Role, s.Status, i, s.Reason)
		}
	}
	chk("reader", ok, "reader", capi.HealthPassing, false)
	lag := ok
	lag.LagSeconds = 60
	chk("lagging", lag, "lagging", capi.HealthWarning, false)
	st := ok
	st.SQLThread = false
	chk("stopped", st, "fenced", capi.HealthCritical, false)
	down := ok
	down.Reachable = false
	chk("down", down, "fenced", capi.HealthCritical, true)
	w := ok
	w.ReadOnly = false
	chk("writable non-primary", w, "fenced", capi.HealthCritical, true)
	nr := ok
	nr.NeedsRebuild = true
	chk("needs rebuild", nr, "fenced", capi.HealthCritical, true)
	io := ok
	io.IOLost = true
	chk("io lost", io, "fenced", capi.HealthCritical, false)
	chk("writer", MemberInput{Node: "kv01", Reachable: true}, "writer", capi.HealthPassing, false)
	chk("writer down", MemberInput{Node: "kv01"}, "writer", capi.HealthCritical, false)
	chk("writer ro", MemberInput{Node: "kv01", Reachable: true, ReadOnly: true}, "writer", capi.HealthCritical, false)
}

func TestMemberDebounce(t *testing.T) {
	d := &memberDebounce{}
	t0 := time.Unix(1000, 0)
	reader := consul.MemberState{Role: "reader", Status: capi.HealthPassing}
	writer := consul.MemberState{Role: "writer", Status: capi.HealthPassing}
	lag := consul.MemberState{Role: "lagging", Status: capi.HealthWarning}
	fen := consul.MemberState{Role: "fenced", Status: capi.HealthCritical}
	des := map[string]consul.MemberState{"kv01": writer, "kv02": reader}
	// new epoch: replica starts fenced, becomes reader only after the delay
	out := d.Step(t0, 5, "kv01", des, nil, 10*time.Second)
	if out["kv02"].Role != "fenced" || out["kv01"].Role != "writer" {
		t.Fatalf("baseline %+v", out)
	}
	out = d.Step(t0.Add(2*time.Second), 5, "kv01", des, nil, 10*time.Second)
	if out["kv02"].Role != "fenced" {
		t.Fatal("must wait")
	}
	out = d.Step(t0.Add(13*time.Second), 5, "kv01", des, nil, 10*time.Second)
	if out["kv02"].Role != "reader" {
		t.Fatal("reader after delay")
	}
	// flap: lagging for 5s then back -> never applied
	des["kv02"] = lag
	d.Step(t0.Add(14*time.Second), 5, "kv01", des, nil, 10*time.Second)
	des["kv02"] = reader
	out = d.Step(t0.Add(19*time.Second), 5, "kv01", des, nil, 10*time.Second)
	if out["kv02"].Role != "reader" {
		t.Fatal("flap must not apply")
	}
	des["kv02"] = lag
	d.Step(t0.Add(20*time.Second), 5, "kv01", des, nil, 10*time.Second)
	out = d.Step(t0.Add(31*time.Second), 5, "kv01", des, nil, 10*time.Second)
	if out["kv02"].Role != "lagging" {
		t.Fatal("persistent lag applies")
	}
	// immediate fence
	des["kv02"] = fen
	out = d.Step(t0.Add(32*time.Second), 5, "kv01", des, map[string]bool{"kv02": true}, 10*time.Second)
	if out["kv02"].Role != "fenced" {
		t.Fatal("immediate fence")
	}
}
